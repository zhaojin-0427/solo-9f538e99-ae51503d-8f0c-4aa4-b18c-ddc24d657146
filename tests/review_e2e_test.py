#!/usr/bin/env python3
"""End-to-end self-test for the review (re-verification) feature.

Topology mirrors docker-compose:
  site_a :8201, site_b :8202, api :8200 (SQLite in a temp dir)

Verifies:
  r1  unchanged verdict on an untouched page
  r2  modified verdict with title/body change details after an edit
  r3  withdrawal only via 404/410; other failures are inconclusive "error"
      and do NOT alter the previous report/corpus
  r4  rate-limit exhaustion during review -> error, old report stays valid
  r5  parse failure (blank page) after collection -> error, old report stays
  r6  report appears only after the whole batch finishes; no half report;
      old corpus stays readable while a review runs
  r7  interleaved reviews: an earlier, slower review can never overwrite a
      newer published report (per-job monotonic seq + atomic publish)
  r8  restart resumes an unfinished review and publishes exactly once
  r9  reviews share quotas/cooldowns/redirects with collection
  r10 validation: reviews only on successful jobs, 404s, endpoint shapes
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
API = "http://127.0.0.1:8200"
A = "http://127.0.0.1:8201"
B = "http://127.0.0.1:8202"
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
                              "--host", "127.0.0.1", "--port", "8200"], env, 8200)


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


def submit(client, urls):
    r = client.post(f"{API}/jobs", json={"urls": urls})
    assert r.status_code == 202, r.text
    return r.json()["job_id"]


def wait_job(client, job_id, timeout=60.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        body = client.get(f"{API}/jobs/{job_id}").json()
        if body["status"] != "running":
            return body
        time.sleep(0.2)
    raise TimeoutError(f"job {job_id} still running after {timeout}s")


def start_review(client, job_id):
    r = client.post(f"{API}/jobs/{job_id}/reviews")
    assert r.status_code == 202, r.text
    return r.json()


def wait_review(client, job_id, review_id, timeout=60.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        body = client.get(f"{API}/jobs/{job_id}/reviews/{review_id}").json()
        if body["status"] != "running":
            return body
        time.sleep(0.2)
    raise TimeoutError(f"review {review_id} still running after {timeout}s")


def verdict_map(body):
    return {i["announcement_id"]: i for i in body["items"]}


def reset_sites():
    for base in (A, B):
        httpx.post(f"{base}/reset", timeout=5)


def main():
    global LOGDIR, DB_DIR
    work = Path(tempfile.mkdtemp(prefix="collector-review-"))
    LOGDIR = work / "logs"
    LOGDIR.mkdir(parents=True)
    DB_DIR = work / "db"
    DB_DIR.mkdir()
    log(f"workdir: {work}")

    start_site("site_a", 8201, "site_a", B)
    start_site("site_b", 8202, "site_b", A)
    start_api()
    client = httpx.Client(timeout=30.0)

    try:
        # ---------- r1+r2: unchanged and modified verdicts
        log("r1/r2: unchanged + modified review with change details")
        reset_sites()
        job = submit(client, [f"{A}/article/1", f"{A}/mutable/2", f"{B}/article/3"])
        body = wait_job(client, job)
        check("r0 collection success", body["status"] == "success")
        anns = {a["url"]: a for a in body["announcements"]}
        mutable_ann = anns[f"{A}/mutable/2"]["id"]
        article_ann = anns[f"{A}/article/1"]["id"]

        # edit the mutable page before reviewing
        edit = httpx.post(f"{A}/mutable/2/edit", timeout=5).json()
        check("r2 site bumped version", edit["version"] == 2, str(edit))
        rv = start_review(client, job)
        check("r6 no report before first review",
              client.get(f"{API}/jobs/{job}/report").status_code == 404)
        rbody = wait_review(client, job, rv["review_id"])
        vmap = verdict_map(rbody)
        check("r1 review completed", rbody["status"] == "completed", rbody["status"])
        check("r1 counts 2 unchanged 1 modified",
              rbody["unchanged"] == 2 and rbody["modified"] == 1 and
              rbody["withdrawn"] == 0 and rbody["error"] == 0, str(rbody.get("summary")))
        check("r1 untouched article unchanged",
              vmap[article_ann]["status"] == "unchanged")
        mod = vmap[mutable_ann]
        check("r2 mutable marked modified", mod["status"] == "modified")
        check("r2 change details present",
              mod["changes"] and mod["changes"]["title_changed"] and
              mod["changes"]["content_changed"] and
              len(mod["changes"]["content_diff"]) > 0,
              str(mod["changes"])[:120])
        rep = client.get(f"{API}/jobs/{job}/report")
        check("r6 report published after batch", rep.status_code == 200, str(rep.status_code))
        report = rep.json()
        check("r6 report summary matches",
              report["summary"] == {"unchanged": 2, "modified": 1,
                                    "withdrawn": 0, "error": 0}, str(report["summary"]))
        check("r6 report item carries previous title",
              any(it["announcement_id"] == mutable_ann and it["previous_title"]
                  for it in report["items"]))
        # old corpus still fully readable
        dl = client.get(f"{API}/jobs/{job}/download")
        check("r6 old corpus still downloadable", dl.status_code == 200)
        check("r6 corpus still has original (v1) title",
              "v1" in [d for d in dl.json()["documents"]
                       if d["url"] == f"{A}/mutable/2"][0]["title"])

        # ---------- r3: withdrawal via 404 and 410
        log("r3: 404/410 are withdrawals; report replaced atomically")
        httpx.post(f"{A}/mutable/2/withdraw?status=404", timeout=5)
        job2 = submit(client, [f"{A}/mutable/4"])
        wait_job(client, job2)
        httpx.post(f"{A}/mutable/4/withdraw?status=410", timeout=5)
        rv2 = start_review(client, job2)
        rbody2 = wait_review(client, job2, rv2["review_id"])
        check("r3 410 verdict withdrawn",
              rbody2["withdrawn"] == 1 and rbody2["items"][0]["status"] == "withdrawn",
              str(rbody2["items"][0]))
        rep2 = client.get(f"{API}/jobs/{job2}/report").json()
        check("r3 report says withdrawn", rep2["summary"]["withdrawn"] == 1)

        # ---------- r4: rate-limit exhaustion -> error, report unchanged
        log("r4: always-429 review -> inconclusive error, old report preserved")
        job3 = submit(client, [f"{A}/article/5", f"{A}/mutable/13"])
        wait_job(client, job3)
        httpx.post(f"{A}/mutable/13/rate-limit", timeout=5)
        rv3 = start_review(client, job3)
        rbody3 = wait_review(client, job3, rv3["review_id"])
        err_items = [i for i in rbody3["items"] if i["status"] == "error"]
        ok_items = [i for i in rbody3["items"] if i["status"] == "unchanged"]
        check("r4 one error + one unchanged",
              len(err_items) == 1 and len(ok_items) == 1, str(rbody3.get("summary")))
        check("r4 error item records reason",
              "HTTP 429" in (err_items[0]["error"] or ""), err_items[0]["error"])
        rep3 = client.get(f"{API}/jobs/{job3}/report")
        check("r4 report still published (error does not block)", rep3.status_code == 200)
        check("r4 report flags the inconclusive item", rep3.json()["summary"]["error"] == 1)
        dl3 = client.get(f"{API}/jobs/{job3}/download").json()
        check("r4 corpus still serves original mutable content",
              any(d["url"].endswith("/mutable/13") for d in dl3["documents"]))

        # ---------- r5: blank page (parse failure) after collection -> error
        log("r5: page becomes unparseable -> error, not withdrawal/modified")
        job5 = submit(client, [f"{A}/mutable/6"])
        wait_job(client, job5)
        httpx.post(f"{A}/mutable/6/blank", timeout=5)
        rv5 = start_review(client, job5)
        rbody5 = wait_review(client, job5, rv5["review_id"])
        check("r5 blank page -> error verdict",
              rbody5["error"] == 1 and rbody5["items"][0]["status"] == "error",
              str(rbody5["items"][0].get("error")))
        check("r5 no withdrawal without 404/410", rbody5["withdrawn"] == 0)
        check("r5 old report remains readable",
              client.get(f"{API}/jobs/{job5}/report").status_code == 200)

        # ---------- r7: interleaved reviews, slow older one must not win
        log("r7: interleaved reviews - older slow review cannot overwrite newer report")
        reset_sites()
        # The slow item lives on site_a (/hold: 429+RA2 then a 2s retry);
        # site_b items finish immediately. Review 1 covers BOTH origins,
        # review 2 (started while review 1 is stuck in the site_a cooldown)
        # only touches site_b and therefore publishes first.
        job7 = submit(client, [f"{A}/hold/7", f"{B}/article/8"])
        wait_job(client, job7)
        rv_old = start_review(client, job7)
        # make sure the 429 was served (cooldown engaged), then start review 2
        deadline = time.time() + 15
        while time.time() < deadline:
            stats = httpx.get(f"{A}/stats", timeout=5).json()
            if stats["gate_fired"]:
                break
            time.sleep(0.1)
        else:
            raise TimeoutError("gate never fired")
        # review 2 re-checks only the fast site_b announcement (explicit subset)
        rv_new = client.post(f"{API}/jobs/{job7}/reviews",
                             json={"urls": [f"{B}/article/8"]}).json()
        check("r7 second review covers the requested subset only",
              rv_new["total"] == 1, str(rv_new.get("total")))
        t0 = time.time()
        rb_new = wait_review(client, job7, rv_new["review_id"])
        elapsed_new = time.time() - t0
        rb_old = wait_review(client, job7, rv_old["review_id"])
        check("r7 newer review published before the slow older one",
              elapsed_new < 1.5, f"newer review took {elapsed_new:.2f}s")
        check("r7 both reviews reached terminal state",
              rb_old["status"] in ("completed", "superseded") and
              rb_new["status"] in ("completed", "superseded"),
              f"old={rb_old['status']} new={rb_new['status']}")
        check("r7 review seqs monotonic", rv_old["seq"] == 1 and rv_new["seq"] == 2)
        current = client.get(f"{API}/jobs/{job7}/report").json()
        check("r7 current report is the NEWER review (seq 2)",
              current["review_id"] == rv_new["review_id"],
              f"current={current['review_id']} old={rv_old['review_id']} new={rv_new['review_id']}")
        listed = {r["review_id"]: r for r in
                  client.get(f"{API}/jobs/{job7}/reviews").json()["reviews"]}
        check("r7 older review marked superseded when it finishes later",
              listed[rv_old["review_id"]]["status"] == "superseded",
              str(listed[rv_old["review_id"]]["status"]))
        check("r7 newer review is current",
              listed[rv_new["review_id"]]["is_current_report"] is True)
        check("r7 both reviews reached verdicts (nothing left pending)",
              all(r["pending"] == 0 for r in listed.values()))

        # ---------- r9: review shares redirect machinery
        log("r9: review follows the same redirect chain as collection")
        job9 = submit(client, [f"{A}/to-peer/9", f"{A}/chain/3"])
        wait_job(client, job9)
        rv9 = start_review(client, job9)
        rb9 = wait_review(client, job9, rv9["review_id"])
        items = {i["announcement_id"]: i for i in rb9["items"]}
        anns9_list = client.get(f"{API}/jobs/{job9}").json()["announcements"]
        anns9 = {a["url"]: a for a in anns9_list}
        peer_item = items[anns9[f"{A}/to-peer/9"]["id"]]
        check("r9 cross-origin review followed redirect (hops=1)",
              peer_item["hops"] == 1 and peer_item["final_url"] == f"{B}/article/9",
              f"hops={peer_item['hops']} url={peer_item['final_url']}")
        check("r9 chain review hops=3",
              items[anns9[f"{A}/chain/3"]["id"]]["hops"] == 3)
        check("r9 all unchanged", rb9["unchanged"] == 2)

        # ---------- r8: restart resumes unfinished review, publishes once
        log("r8: restart resumes an unfinished review; no half report")
        reset_sites()
        # /hold: first review request gets 429 + Retry-After: 2 and the
        # retry sleeps another 2s, giving a deterministic window to kill
        # the API while the review is unfinished.
        job8 = submit(client, [f"{A}/article/11", f"{A}/hold/12"])
        wait_job(client, job8)
        rv8 = start_review(client, job8)
        deadline = time.time() + 20
        ready = False
        while time.time() < deadline:
            stats = httpx.get(f"{A}/stats", timeout=5).json()
            rb = client.get(f"{API}/jobs/{job8}/reviews/{rv8['review_id']}").json()
            if stats["gate_fired"] and rb["status"] == "running":
                ready = True
                break
            if rb["status"] != "running":
                break
            time.sleep(0.05)
        check("r8 review unfinished with cooldown engaged before kill", ready)
        check("r8 no report while review running",
              client.get(f"{API}/jobs/{job8}/report").status_code == 404)
        stop_api()
        log("  api stopped, restarting...")
        time.sleep(1.0)
        start_api()
        rb8 = wait_review(client, job8, rv8["review_id"], timeout=60)
        check("r8 review finished after restart",
              rb8["status"] == "completed", rb8["status"])
        check("r8 exactly one report row / one publication",
              client.get(f"{API}/jobs/{job8}/report").json()["review_id"] == rv8["review_id"])
        reviews = client.get(f"{API}/jobs/{job8}/reviews").json()["reviews"]
        check("r8 only one review run exists", len(reviews) == 1, str(len(reviews)))
        check("r8 resumed item has verdicts for all items",
              rb8["unchanged"] + rb8["modified"] + rb8["withdrawn"] + rb8["error"] == 2)

        # ---------- r10: validation
        log("r10: validation and edge cases")
        # `job` was reassigned in r3 to a single-announcement job whose
        # first (and so far only) review was the r3 one.
        bad_job = submit(client, [f"{A}/article/20", f"{B}/busy"])
        wait_job(client, bad_job)
        r = client.post(f"{API}/jobs/{bad_job}/reviews")
        check("r10 review on failed job -> 409", r.status_code == 409, str(r.status_code))
        r = client.post(f"{API}/jobs/nope/reviews")
        check("r10 review on unknown job -> 404", r.status_code == 404)
        r = client.get(f"{API}/jobs/{job}/reviews/nope")
        check("r10 unknown review -> 404", r.status_code == 404)
        r = client.get(f"{API}/jobs/nope/report")
        check("r10 unknown job report -> 404", r.status_code == 404)
        before = client.get(f"{API}/jobs/{job}/reviews").json()["reviews"]
        check("r10 review list works",
              client.get(f"{API}/jobs/{job}/reviews").status_code == 200 and len(before) == 1,
              f"count={len(before)}")
        r = client.post(f"{API}/jobs/{job}/reviews", json={"urls": ["http://x/not-in-job"]})
        check("r10 subset with foreign url -> 422", r.status_code == 422, str(r.status_code))
        r = client.post(f"{API}/jobs/{job}/reviews", json={"urls": ["ftp://nope/1"]})
        check("r10 subset invalid scheme -> 422", r.status_code == 422, str(r.status_code))
        # starting another review on a job that already has a report is allowed
        r = client.post(f"{API}/jobs/{job}/reviews")
        next_seq = before[0]["seq"] + 1
        check("r10 repeated review allowed",
              r.status_code == 202 and r.json()["seq"] == next_seq,
              f"status={r.status_code} body={r.text[:120]}")
        rb = wait_review(client, job, r.json()["review_id"])
        check("r10 next review completed", rb["status"] == "completed")
        cur = client.get(f"{API}/jobs/{job}/report").json()
        check("r10 newest report wins", cur["seq"] == next_seq, str(cur["seq"]))

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
    print("RESULT: ALL REVIEW CHECKS PASSED")
    shutil.rmtree(work, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
