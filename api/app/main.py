"""FastAPI application: submit jobs, query progress, download full corpus,
and run review (re-verification) passes over successful jobs."""
import json
import logging
import os
import time
import uuid
from collections import Counter
from contextlib import asynccontextmanager
from urllib.parse import urlparse

from fastapi import Body, FastAPI, HTTPException
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel, Field, field_validator

from .db import DB
from .engine import COLLECT, REVIEW, FetchEngine
from .extract import extract_document

DB_PATH = os.environ.get("DB_PATH", "./collector.db")

log = logging.getLogger("collector.api")


def build_review_report(db: DB, review_id: str) -> dict:
    """Assemble the immutable JSON report for a fully terminal review."""
    review = db.get_review(review_id)
    items = db.list_review_items(review_id)
    counts = Counter(i["status"] for i in items)
    entries = []
    for i in items:
        original = db.get_announcement(i["ann_id"])
        entry = {
            "review_item_id": i["id"],
            "announcement_id": i["ann_id"],
            "original_url": i["url"],
            "final_url": i["current_url"],
            "verdict": i["status"],  # unchanged|modified|withdrawn|error
            "hops": i["hops"],
            "attempts": i["attempts"],
            "previous_title": original["title"],
            "current_title": i["title"],
            "error": i["error"],
        }
        if i["status"] == "modified":
            entry["changes"] = json.loads(i["changes"]) if i["changes"] else None
        else:
            entry["changes"] = None
        entries.append(entry)
    return {
        "job_id": review["job_id"],
        "review_id": review_id,
        "seq": review["seq"],
        "generated_at": time.time(),
        "total": len(items),
        "summary": {
            "unchanged": counts.get("unchanged", 0),
            "modified": counts.get("modified", 0),
            "withdrawn": counts.get("withdrawn", 0),
            "error": counts.get("error", 0),
        },
        "items": entries,
    }


def publish_completed_review(db: DB, review_id: str) -> bool:
    """Build and atomically publish a completed review's report.

    Returns True if it became the current report, False if a newer review
    had already been published (this run is then marked superseded).
    """
    report = build_review_report(db, review_id)
    return db.publish_review_if_newest(
        review_id, json.dumps(report, ensure_ascii=False))


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
    engine = FetchEngine(db, extract_document,
                         review_completed_cb=lambda rid: publish_completed_review(db, rid))
    await engine.start()
    app.state.db = db
    app.state.engine = engine

    # Resume unfinished announcements; successful ones are NOT re-fetched.
    resumed = 0
    for row in db.unfinished_announcements():
        db.update_ann(row["id"], status="pending")
        engine.submit(COLLECT, row["id"], row["current_url"], row["hops"], row["attempts"])
        resumed += 1

    # Resume unfinished review runs. Every unfinished item is restarted
    # from its ORIGINAL URL (fresh hops/attempts); reviews that never
    # published leave no half report behind.
    resumed_reviews = 0
    for rv in db.unfinished_reviews():
        review_id = rv["id"]
        for row in db.review_items_unfinished(review_id):
            item = db.get_review_item(row["id"])
            db.update_review_item(row["id"], status="pending",
                                  current_url=item["url"], hops=0, attempts=0, error=None)
            engine.submit(REVIEW, row["id"], item["url"], 0, 0, review_id)
            resumed_reviews += 1
    log.info("startup: resumed %d unfinished announcement(s) and %d review item(s); "
             "active cooldowns: %s",
             resumed, resumed_reviews, engine.cooldowns_snapshot())
    yield
    await engine.stop()
    db.close()


app = FastAPI(
    title="Tender Corpus Collector",
    version="1.1.0",
    description=(
        "Collect tender announcements from multiple official sites and "
        "re-verify (review) successful jobs by re-fetching the original URLs. "
        "Collection and review share the same quotas (2 in-flight requests "
        "globally, 1 per origin), per-origin cooldowns on 429/503 + "
        "Retry-After, max 3 redirect hops and max 2 retries per URL. "
        "A review report classifies every announcement as unchanged, "
        "modified or withdrawn (only 404/410); rate-limit/network/parse "
        "failures are recorded as inconclusive errors and never alter the "
        "previous report. Reports are published atomically only after the "
        "whole review batch finishes, and older, slower reviews can never "
        "overwrite a newer published report."
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


class ReviewCreate(BaseModel):
    urls: list[str] | None = Field(
        default=None,
        description="Optional subset of original URLs to re-check; "
                    "defaults to every announcement of the job",
    )

    @field_validator("urls")
    @classmethod
    def validate_urls(cls, urls):
        if urls is None:
            return None
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
        engine.submit(COLLECT, ann_id, url, hops=0, attempts=0)
    return {
        "job_id": job_id,
        "total": len(payload.urls),
        "status": "running",
        "progress_url": f"/jobs/{job_id}",
        "download_url": f"/jobs/{job_id}/download",
        "review_url": f"/jobs/{job_id}/reviews",
    }


@app.get("/jobs", summary="List all jobs with progress counters")
async def list_jobs():
    rows = app.state.db.list_jobs()
    jobs = []
    for r in rows:
        status = "running" if r["unfinished"] else ("failed" if r["failed"] else "success")
        report = app.state.db.get_report(r["id"])
        jobs.append({
            "job_id": r["id"], "created_at": r["created_at"], "total": r["total"],
            "success": r["success"], "failed": r["failed"], "unfinished": r["unfinished"],
            "status": status,
            "current_report_review_id": report["review_id"] if report else None,
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
    db: DB = app.state.db
    anns = db.list_announcements(job_id)
    counts = Counter(a["status"] for a in anns)
    if counts["pending"] or counts["fetching"]:
        status = "running"
    elif counts["failed"]:
        status = "failed"
    else:
        status = "success"
    report = db.get_report(job_id)
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
        "current_report_review_id": report["review_id"] if report else None,
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


# ----------------------------------------------------------------------
# Review (re-verification) API
# ----------------------------------------------------------------------
@app.post("/jobs/{job_id}/reviews", status_code=202,
          summary="Start a review: re-fetch announcements of a successful job")
async def start_review(job_id: str, payload: ReviewCreate | None = Body(default=None)):
    db: DB = app.state.db
    engine: FetchEngine = app.state.engine
    _job_or_404(job_id)
    anns = db.list_announcements(job_id)
    if not anns:
        raise HTTPException(status_code=404, detail="job has no announcements")
    if any(a["status"] != "success" for a in anns):
        raise HTTPException(
            status_code=409,
            detail="reviews can only run on fully successful jobs; "
                   "failed or unfinished announcements have no trusted baseline",
        )
    selected = anns
    if payload is not None and payload.urls is not None:
        by_url = {a["url"]: a for a in anns}
        unknown = [u for u in payload.urls if u not in by_url]
        if unknown:
            raise HTTPException(
                status_code=422,
                detail=f"URLs not part of this job's original corpus: {unknown}",
            )
        selected = [by_url[u] for u in payload.urls]
    review_id = uuid.uuid4().hex[:12]
    seq = db.next_review_seq(job_id)
    items = [(a["id"], a["url"]) for a in selected]
    db.create_review(review_id, job_id, seq, items)
    # Re-fetch from the ORIGINAL url; redirects/cooldowns/retries are the
    # engine's shared machinery.
    created = {i["ann_id"]: i for i in db.list_review_items(review_id)}
    for ann_id, url in items:
        engine.submit(REVIEW, created[ann_id]["id"], url, hops=0, attempts=0,
                      review_id=review_id)
    log.info("started review %s seq=%d for job %s (%d items)",
             review_id, seq, job_id, len(items))
    return {
        "review_id": review_id,
        "job_id": job_id,
        "seq": seq,
        "status": "running",
        "total": len(items),
        "progress_url": f"/jobs/{job_id}/reviews/{review_id}",
        "report_url": f"/jobs/{job_id}/report",
    }


@app.get("/jobs/{job_id}/reviews", summary="List all review runs of a job")
async def list_reviews(job_id: str):
    _job_or_404(job_id)
    db: DB = app.state.db
    report = db.get_report(job_id)
    out = []
    for rv in db.list_reviews(job_id):
        items = db.list_review_items(rv["id"])
        counts = Counter(i["status"] for i in items)
        out.append({
            "review_id": rv["id"],
            "seq": rv["seq"],
            "status": rv["status"],  # running|completed|superseded
            "created_at": rv["created_at"],
            "finished_at": rv["finished_at"],
            "total": rv["total"],
            "pending": counts.get("pending", 0) + counts.get("fetching", 0),
            "unchanged": counts.get("unchanged", 0),
            "modified": counts.get("modified", 0),
            "withdrawn": counts.get("withdrawn", 0),
            "error": counts.get("error", 0),
            "is_current_report": report is not None and report["review_id"] == rv["id"],
        })
    return {"job_id": job_id, "reviews": out}


@app.get("/jobs/{job_id}/reviews/{review_id}",
         summary="Review progress with per-announcement verdicts")
async def review_progress(job_id: str, review_id: str):
    _job_or_404(job_id)
    db: DB = app.state.db
    rv = db.get_review(review_id)
    if rv is None or rv["job_id"] != job_id:
        raise HTTPException(status_code=404, detail="review not found")
    items = db.list_review_items(review_id)
    counts = Counter(i["status"] for i in items)
    running = counts.get("pending", 0) + counts.get("fetching", 0)
    report = db.get_report(job_id)
    return {
        "review_id": review_id,
        "job_id": job_id,
        "seq": rv["seq"],
        "status": "running" if running else rv["status"],
        "created_at": rv["created_at"],
        "finished_at": rv["finished_at"],
        "total": len(items),
        "pending": counts.get("pending", 0),
        "fetching": counts.get("fetching", 0),
        "unchanged": counts.get("unchanged", 0),
        "modified": counts.get("modified", 0),
        "withdrawn": counts.get("withdrawn", 0),
        "error": counts.get("error", 0),
        "published_as_current_report": report is not None and report["review_id"] == review_id,
        "items": [
            {
                "review_item_id": i["id"],
                "announcement_id": i["ann_id"],
                "original_url": i["url"],
                "final_url": i["current_url"],
                "status": i["status"],
                "hops": i["hops"],
                "attempts": i["attempts"],
                "current_title": i["title"],
                "changes": json.loads(i["changes"]) if i["changes"] else None,
                "error": i["error"],
                "updated_at": i["updated_at"],
            }
            for i in items
        ],
    }


@app.get("/jobs/{job_id}/report",
         summary="Read the currently published review report (404 until one exists)")
async def get_report(job_id: str):
    _job_or_404(job_id)
    row = app.state.db.get_report(job_id)
    if row is None:
        raise HTTPException(
            status_code=404,
            detail="no review report published yet; reports appear only after a "
                   "whole review batch finishes",
        )
    return JSONResponse(
        content=json.loads(row["payload"]),
        headers={"Content-Disposition":
                 f'attachment; filename="review-report-{job_id}-{row["review_id"]}.json"'},
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
