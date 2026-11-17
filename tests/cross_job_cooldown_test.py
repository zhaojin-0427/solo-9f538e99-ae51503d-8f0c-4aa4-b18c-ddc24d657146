#!/usr/bin/env python3
"""Focused test: a cooldown triggered by job X delays job Y's unsent
requests to the same origin (shared quota/cooldown across jobs)."""
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
API = "http://127.0.0.1:8100"
A = "http://127.0.0.1:8101"
B = "http://127.0.0.1:8102"
PY = sys.executable
work = Path(tempfile.mkdtemp(prefix="collector-xjob-"))
procs = []


def start(name, args, env, port):
    f = open(work / f"{name}.log", "w")
    p = subprocess.Popen(args, env=env, stdout=f, stderr=subprocess.STDOUT)
    procs.append(p)
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            httpx.get(f"http://127.0.0.1:{port}/", timeout=1.0)
            return
        except Exception:
            time.sleep(0.2)
    raise RuntimeError(name)


try:
    start("site_a", [PY, "-m", "uvicorn", "app:app", "--app-dir", str(ROOT / "sites"),
                     "--port", "8101"], dict(os.environ, SITE_NAME="site_a", PEER_BASE=B), 8101)
    start("site_b", [PY, "-m", "uvicorn", "app:app", "--app-dir", str(ROOT / "sites"),
                     "--port", "8102"], dict(os.environ, SITE_NAME="site_b", PEER_BASE=A), 8102)
    start("api", [PY, "-m", "uvicorn", "app.main:app", "--app-dir", str(ROOT / "api"),
                  "--port", "8100"], dict(os.environ, DB_PATH=str(work / "c.db")), 8100)

    c = httpx.Client(timeout=30.0)
    t0 = time.time()
    # job X: triggers a 5s cooldown on site_a
    jx = c.post(f"{API}/jobs", json={"urls": [f"{A}/flaky/1?fails=1&ra=5"]}).json()["job_id"]
    # wait until the 429 was received (cooldown set), then submit job Y to the SAME origin
    deadline = time.time() + 10
    while time.time() < deadline:
        hits = httpx.get(f"{A}/stats").json()["hits"]
        if hits.get("/flaky/1"):
            break
        time.sleep(0.1)
    jy = c.post(f"{API}/jobs", json={"urls": [f"{A}/article/1", f"{B}/article/2"]}).json()["job_id"]

    done_y = {}
    while True:
        body = c.get(f"{API}/jobs/{jy}").json()
        for a in body["announcements"]:
            if a["status"] == "success" and a["url"] not in done_y:
                done_y[a["url"]] = time.time() - t0
        if body["status"] != "running":
            break
        if time.time() - t0 > 30:
            raise TimeoutError("timeout")
        time.sleep(0.1)

    bx = c.get(f"{API}/jobs/{jx}").json()
    a_art = done_y[f"{A}/article/1"]
    b_art = done_y[f"{B}/article/2"]
    print(f"job Y site_a article done at {a_art:.2f}s (cooldown 5s set by job X)")
    print(f"job Y site_b article done at {b_art:.2f}s (other origin unaffected)")
    print(f"job X status: {bx['status']}")
    ok = True
    ok &= a_art >= 4.0          # delayed by job X's cooldown
    ok &= b_art <= 2.5          # other origin not delayed
    ok &= bx["status"] == "success"
    print("RESULT:", "PASS" if ok else "FAIL")
    sys.exit(0 if ok else 1)
finally:
    for p in procs:
        if p.poll() is None:
            p.terminate()
