#!/usr/bin/env python3
"""End-to-end self-test for the tender corpus collector.

Spins up the real services as local processes (mirroring docker-compose):
  site_a :8001, site_b :8002, site_c :8003 (extra origin for global-quota test),
  api    :8000 (SQLite DB under a temp dir)

Then verifies: extraction, cross-origin redirects, hop limit, 429/503 +
Retry-After cooldowns (shared across jobs, not blocking other origins, not
consuming global slots), retry budget, global/per-origin concurrency,
download gating, and restart persistence (cooldowns kept, unfinished
resumed, successful not re-fetched).
"""
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
API = "http://127.0.0.1:8000"
A = "http://127.0.0.1:8001"
B = "http://127.0.0.1:8002"
C = "http://127.0.0.1:8003"

PY = sys.executable
FAILURES = []
PROCS = []
LOGDIR = None
DB_DIR = None


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def check(name, cond, detail=""):
    mark = "PASS" if cond else "FAIL"
    print(f"  [{mark}] {name}" + (f"  ({detail})" if detail else ""), flush=True)
    if not cond:
        FAILURES.append(f"{name} {detail}")


def start_proc(name, args, env, port):
    logfile = open(LOGDIR / f"{name}.log", "w")
    p = subprocess.Popen(args, env=env, stdout=logfile, stderr=subprocess.STDOUT)
    PROCS.append((name, p))
    deadline = time.time() + 30
    while time.time() < deadline:
        if p.poll() is not None:
            raise RuntimeError(f"{name} exited early, see {LOGDIR}/{name}.log")
        try:
            httpx.get(f"http://127.0.0.1:{port}/", timeout=1.0)
            return p
        except Exception:
            time.sleep(0.2)
    raise RuntimeError(f"{name} did not start on port {port}")


def start_api():
    env = dict(os.environ, DB_PATH=str(DB_DIR / "collector.db"))
    return start_proc("api", [PY, "-m", "uvicorn", "app.main:app", "--app-dir", str(ROOT / "api"),
                              "--host", "127.0.0.1", "--port", "8000"], env, 8000)


def start_site(name, port, site_name, peer_base):
    env = dict(os.environ, SITE_NAME=site_name, PEER_BASE=peer_base)
    return start_proc(name, [PY, "-m", "uvicorn", "app:app", "--app-dir", str(ROOT / "sites"),
                             "--host", "127.0.0.1", "--port", str(port)], env, port)


def stop_api():
    for i, (name, p) in enumerate(PROCS):
        if name == "api" and p.poll() is None:
            p.send_signal(signal.SIGTERM)
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
            PROCS.pop(i)
            return


def wait_job(client, job_id, timeout=60.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        r = client.get(f"{API}/jobs/{job_id}")
        r.raise_for_status()
        body = r.json()
        if body["status"] != "running":
            return body, time.time() - t0
        time.sleep(0.2)
    raise TimeoutError(f"job {job_id} still running after {timeout}s")


def submit(client, urls):
    r = client.post(f"{API}/jobs", json={"urls": urls})
    assert r.status_code == 202, r.text
    return r.json()["job_id"]


def site_stats(base):
    return httpx.get(f"{base}/stats", timeout=5).json()["hits"]


def reset_sites():
    for base in (A, B, C):
        httpx.post(f"{base}/reset", timeout=5)


def main():
    global LOGDIR, DB_DIR
    work = Path(tempfile.mkdtemp(prefix="collector-e2e-"))
    LOGDIR = work / "logs"
    LOGDIR.mkdir(parents=True)
    DB_DIR = work / "db"
    DB_DIR.mkdir()
    log(f"workdir: {work}")

    start_site("site_a", 8001, "site_a", B)
    start_site("site_b", 8002, "site_b", A)
    start_site("site_c", 8003, "site_c", C)
    start_api()
    client = httpx.Client(timeout=30.0)

    try:
        # ---------- phase 1: extraction + cross-origin redirects + hop limit ok
        log("phase 1: extraction, cross-site redirect, 3-hop chain")
        reset_sites()
        job = submit(client, [f"{A}/article/1", f"{A}/to-peer/2", f"{A}/chain/3", f"{B}/article/5"])
        body, _ = wait_job(client, job)
        check("p1 job success", body["status"] == "success", str(body["status"]))
        anns = {a["url"]: a for a in body["announcements"]}
        check("p1 direct hops=0", anns[f"{A}/article/1"]["hops"] == 0)
        check("p1 to-peer hops=1", anns[f"{A}/to-peer/2"]["hops"] == 1)
        check("p1 chain/3 hops=3", anns[f"{A}/chain/3"]["hops"] == 3)
        check("p1 to-peer landed on site_b",
              anns[f"{A}/to-peer/2"]["current_url"] == f"{B}/article/2")
        r = client.get(f"{API}/jobs/{job}/download")
        check("p1 download 200", r.status_code == 200, str(r.status_code))
        docs = {d["url"]: d for d in r.json()["documents"]}
        d1 = docs[f"{A}/article/1"]
        check("p1 title extracted", "招标公告 1 号" in d1["title"], d1["title"])
        check("p1 content extracted", "预算金额" in d1["content"])
        check("p1 script stripped", "tracker" not in d1["content"])
        check("p1 cross-site doc from site_b",
              "site_b" in docs[f"{A}/to-peer/2"]["title"], docs[f"{A}/to-peer/2"]["title"])

        # ---------- phase 2: hop limit exceeded + redirect loop -> failed, no download
        log("phase 2: hop limit and redirect loop")
        reset_sites()
        job = submit(client, [f"{A}/chain/4", f"{B}/redirect-loop"])
        body, _ = wait_job(client, job)
        check("p2 job failed", body["status"] == "failed")
        errs = {a["url"]: a["error"] for a in body["announcements"]}
        check("p2 chain/4 too many redirects",
              "too many redirects" in (errs[f"{A}/chain/4"] or ""), errs[f"{A}/chain/4"])
        check("p2 redirect-loop too many redirects",
              "too many redirects" in (errs[f"{B}/redirect-loop"] or ""))
        r = client.get(f"{API}/jobs/{job}/download")
        check("p2 download blocked (409)", r.status_code == 409, str(r.status_code))

        # ---------- phase 3: 429+Retry-After cooldown; other origin unaffected
        log("phase 3: cooldown on 429+Retry-After, other origin keeps going")
        reset_sites()
        t0 = time.time()
        job = submit(client, [f"{A}/flaky/1?fails=2&ra=3", f"{B}/article/7", f"{B}/slow/1?delay=2"])
        seen = {}
        cd_seen = False
        while True:
            body = client.get(f"{API}/jobs/{job}").json()
            for a in body["announcements"]:
                if a["status"] == "success" and a["url"] not in seen:
                    seen[a["url"]] = time.time() - t0
            if client.get(f"{API}/cooldowns").json()["cooldowns"]:
                cd_seen = True
            if body["status"] != "running":
                break
            if time.time() - t0 > 40:
                raise TimeoutError("phase 3 timeout")
            time.sleep(0.15)
        check("p3 job success", body["status"] == "success")
        check("p3 cooldown endpoint active", cd_seen)
        check("p3 site_b article done during site_a cooldown",
              seen.get(f"{B}/article/7", 99) < 2.0, f"t={seen.get(f'{B}/article/7'):.2f}s")
        check("p3 site_b slow done during site_a cooldown",
              seen.get(f"{B}/slow/1?delay=2", 99) < 4.5, f"t={seen.get(f'{B}/slow/1?delay=2'):.2f}s")
        flaky_t = seen.get(f"{A}/flaky/1?fails=2&ra=3", 0)
        check("p3 flaky waited for two 3s cooldowns", flaky_t >= 4.5, f"t={flaky_t:.2f}s")
        fa = [a for a in body["announcements"] if a["url"].startswith(f"{A}/flaky")][0]
        check("p3 flaky used 2 retries", fa["attempts"] == 2, f"attempts={fa['attempts']}")
        hits_a = site_stats(A)
        check("p3 flaky got 3 requests", len(hits_a.get("/flaky/1", [])) == 3,
              str(hits_a.get("/flaky/1")))

        # ---------- phase 4: retry exhaustion
        log("phase 4: retry budget exhausted -> failed")
        reset_sites()
        job = submit(client, [f"{B}/busy", f"{A}/flaky-no-ra"])
        body, _ = wait_job(client, job)
        check("p4 job failed", body["status"] == "failed")
        for a in body["announcements"]:
            check(f"p4 {a['url'].split('/')[-1]} attempts=2", a["attempts"] == 2)
            check(f"p4 {a['url'].split('/')[-1]} retries-exhausted error",
                  "retries exhausted" in (a["error"] or ""), a["error"])
        check("p4 /busy requested exactly 3 times",
              len(site_stats(B).get("/busy", [])) == 3, str(site_stats(B).get("/busy")))
        check("p4 download blocked (409)",
              client.get(f"{API}/jobs/{job}/download").status_code == 409)

        # ---------- phase 5: concurrency quotas
        log("phase 5: per-origin serial + global quota = 2")
        reset_sites()
        t0 = time.time()
        job = submit(client, [f"{A}/slow/1?delay=2", f"{A}/slow/2?delay=2",
                              f"{B}/slow/3?delay=2", f"{B}/slow/4?delay=2"])
        body, dur = wait_job(client, job, timeout=30)
        check("p5a two origins x two 2s-slow: serial per origin, parallel across origins",
              3.5 <= dur <= 7.0, f"{dur:.2f}s")
        hits = site_stats(A)
        span = hits["/slow/2"][0] - hits["/slow/1"][0]
        check("p5a same-origin requests serialized", span >= 1.8, f"gap={span:.2f}s")

        reset_sites()
        t0 = time.time()
        job = submit(client, [f"{A}/slow/5?delay=2", f"{B}/slow/6?delay=2", f"{C}/slow/7?delay=2"])
        body, dur = wait_job(client, job, timeout=30)
        check("p5b three origins x 2s-slow: global quota 2 forces two waves",
              dur >= 3.2, f"{dur:.2f}s")
        check("p5b job success", body["status"] == "success")

        # ---------- phase 6: restart persistence
        log("phase 6: restart keeps cooldowns, resumes unfinished, skips successful")
        reset_sites()
        job = submit(client, [f"{A}/article/11", f"{A}/flaky/9?fails=1&ra=12", f"{B}/article/12"])
        # wait until article/11 succeeded and flaky got its first (429) hit
        deadline = time.time() + 20
        while time.time() < deadline:
            hits_a = site_stats(A)
            body = client.get(f"{API}/jobs/{job}").json()
            a11 = [x for x in body["announcements"] if x["url"].endswith("/article/11")][0]
            if a11["status"] == "success" and len(hits_a.get("/flaky/9", [])) == 1:
                break
            time.sleep(0.15)
        else:
            raise TimeoutError("phase 6 precondition timeout")
        t_kill = time.time()
        stop_api()
        log("  api stopped, restarting...")
        time.sleep(1.0)
        start_api()
        body, _ = wait_job(client, job, timeout=60)
        check("p6 job success after restart", body["status"] == "success")
        hits_a = site_stats(A)
        check("p6 successful article/11 NOT re-fetched",
              len(hits_a.get("/article/11", [])) == 1, str(hits_a.get("/article/11")))
        f9 = hits_a.get("/flaky/9", [])
        check("p6 flaky fetched exactly twice", len(f9) == 2, str(f9))
        if len(f9) == 2:
            check("p6 cooldown survived restart (retry waited ~12s, not ~instant)",
                  f9[1] - f9[0] >= 8.0, f"gap={f9[1] - f9[0]:.2f}s")
        r = client.get(f"{API}/jobs/{job}/download")
        check("p6 download 200 after restart", r.status_code == 200)
        check("p6 corpus has 3 documents", len(r.json()["documents"]) == 3)

        # ---------- phase 7: validation
        log("phase 7: input validation")
        r = client.post(f"{API}/jobs", json={"urls": [f"{A}/article/{i}" for i in range(21)]})
        check("p7 >20 urls rejected", r.status_code == 422, str(r.status_code))
        r = client.post(f"{API}/jobs", json={"urls": ["ftp://x/1"]})
        check("p7 non-http url rejected", r.status_code == 422)
        r = client.post(f"{API}/jobs", json={"urls": []})
        check("p7 empty list rejected", r.status_code == 422)
        r = client.post(f"{API}/jobs", json={"urls": [f"{A}/article/3", f"{A}/article/3"]})
        check("p7 duplicates deduped", r.status_code == 202 and r.json()["total"] == 1)
        r = client.get(f"{API}/jobs/doesnotexist")
        check("p7 unknown job 404", r.status_code == 404)
        r = client.get(f"{API}/jobs")
        check("p7 job list works", r.status_code == 200 and len(r.json()["jobs"]) >= 7)

    finally:
        for name, p in PROCS:
            if p.poll() is None:
                p.send_signal(signal.SIGTERM)
        time.sleep(0.5)
        for name, p in PROCS:
            if p.poll() is None:
                p.kill()

    print()
    if FAILURES:
        print(f"RESULT: {len(FAILURES)} FAILURE(S)")
        for f in FAILURES:
            print(f"  - {f}")
        print(f"logs kept at {LOGDIR}")
        return 1
    print("RESULT: ALL CHECKS PASSED")
    shutil.rmtree(work, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
