"""FastAPI application: submit jobs, query progress, download full corpus."""
import logging
import os
import time
import uuid
from collections import Counter
from contextlib import asynccontextmanager
from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel, Field, field_validator

from .db import DB
from .engine import FetchEngine
from .extract import extract_document

DB_PATH = os.environ.get("DB_PATH", "./collector.db")

log = logging.getLogger("collector.api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    collector_logger = logging.getLogger("collector")
    collector_logger.setLevel(logging.INFO)
    if not collector_logger.handlers:
        collector_logger.addHandler(handler)

    db_dir = os.path.dirname(os.path.abspath(DB_PATH))
    os.makedirs(db_dir, exist_ok=True)
    db = DB(DB_PATH)
    engine = FetchEngine(db, extract_document)
    await engine.start()
    app.state.db = db
    app.state.engine = engine

    # Resume unfinished announcements; successful ones are NOT re-fetched.
    resumed = 0
    for row in db.unfinished_announcements():
        db.update_ann(row["id"], status="pending")
        engine.submit(row["id"], row["current_url"], row["hops"], row["attempts"])
        resumed += 1
    log.info("startup: resumed %d unfinished announcement(s), active cooldowns: %s",
             resumed, engine.cooldowns_snapshot())
    yield
    await engine.stop()
    db.close()


app = FastAPI(
    title="Tender Corpus Collector",
    version="1.0.0",
    description=(
        "Collect tender announcements from multiple official sites. "
        "Submit up to 20 announcement URLs per job; the collector extracts "
        "HTML title and body text with shared quotas (2 in-flight requests "
        "globally, 1 per origin), per-origin cooldowns on 429/503 + "
        "Retry-After, max 3 redirect hops and max 2 retries per URL. "
        "The full JSON corpus is downloadable only when every announcement "
        "of a job succeeded."
    ),
    lifespan=lifespan,
)


class JobCreate(BaseModel):
    urls: list[str] = Field(min_length=1, max_length=20,
                            description="1-20 announcement URLs (http/https)")

    @field_validator("urls")
    @classmethod
    def validate_and_dedupe(cls, urls):
        seen, out = set(), []
        for raw in urls:
            u = raw.strip()
            p = urlparse(u)
            if p.scheme not in ("http", "https") or not p.netloc:
                raise ValueError(f"invalid URL (http/https required): {raw!r}")
            if u not in seen:
                seen.add(u)
                out.append(u)
        if not out:
            raise ValueError("no usable URLs")
        return out


@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/docs")


@app.post("/jobs", status_code=202, summary="Submit a collection job (up to 20 URLs)")
async def create_job(payload: JobCreate):
    db: DB = app.state.db
    engine: FetchEngine = app.state.engine
    job_id = uuid.uuid4().hex[:12]
    db.create_job(job_id, total=len(payload.urls))
    for url in payload.urls:
        ann_id = db.add_announcement(job_id, url)
        engine.submit(ann_id, url, hops=0, attempts=0)
    return {
        "job_id": job_id,
        "total": len(payload.urls),
        "status": "running",
        "progress_url": f"/jobs/{job_id}",
        "download_url": f"/jobs/{job_id}/download",
    }


@app.get("/jobs", summary="List all jobs with progress counters")
async def list_jobs():
    rows = app.state.db.list_jobs()
    jobs = []
    for r in rows:
        status = "running" if r["unfinished"] else ("failed" if r["failed"] else "success")
        jobs.append({
            "job_id": r["id"], "created_at": r["created_at"], "total": r["total"],
            "success": r["success"], "failed": r["failed"], "unfinished": r["unfinished"],
            "status": status,
        })
    return {"jobs": jobs}


def _job_or_404(job_id: str):
    job = app.state.db.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job


@app.get("/jobs/{job_id}", summary="Job progress with per-announcement status")
async def job_progress(job_id: str):
    job = _job_or_404(job_id)
    anns = app.state.db.list_announcements(job_id)
    counts = Counter(a["status"] for a in anns)
    if counts["pending"] or counts["fetching"]:
        status = "running"
    elif counts["failed"]:
        status = "failed"
    else:
        status = "success"
    return {
        "job_id": job_id,
        "status": status,
        "created_at": job["created_at"],
        "total": len(anns),
        "pending": counts["pending"],
        "fetching": counts["fetching"],
        "success": counts["success"],
        "failed": counts["failed"],
        "downloadable": status == "success",
        "announcements": [
            {
                "id": a["id"],
                "url": a["url"],
                "current_url": a["current_url"],
                "status": a["status"],
                "hops": a["hops"],
                "attempts": a["attempts"],
                "title": a["title"],
                "error": a["error"],
                "updated_at": a["updated_at"],
            }
            for a in anns
        ],
    }


@app.get("/jobs/{job_id}/download", summary="Download full JSON corpus (only when ALL succeeded)")
async def download_corpus(job_id: str):
    _job_or_404(job_id)
    anns = app.state.db.list_announcements(job_id)
    not_done = [a for a in anns if a["status"] != "success"]
    if not_done:
        counts = Counter(a["status"] for a in anns)
        raise HTTPException(
            status_code=409,
            detail=(
                "corpus is only available when every announcement succeeded; "
                f"current counts: {dict(counts)}. Partial results are not provided."
            ),
        )
    documents = [
        {
            "url": a["url"],
            "final_url": a["current_url"],
            "hops": a["hops"],
            "title": a["title"],
            "content": a["content"],
        }
        for a in anns
    ]
    payload = {
        "job_id": job_id,
        "generated_at": time.time(),
        "document_count": len(documents),
        "documents": documents,
    }
    return JSONResponse(
        content=payload,
        headers={"Content-Disposition": f'attachment; filename="corpus-{job_id}.json"'},
    )


@app.get("/cooldowns", summary="Currently active per-origin cooldowns")
async def cooldowns():
    now = time.time()
    active = [
        {"origin": origin, "until_ts": until, "remaining_seconds": round(max(0.0, until - now), 3)}
        for origin, until in app.state.engine.cooldowns_snapshot().items()
        if until > now
    ]
    return {"cooldowns": active}
