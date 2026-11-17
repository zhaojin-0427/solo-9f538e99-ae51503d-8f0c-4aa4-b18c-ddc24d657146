"""Mock tender-announcement site.

One code base serves both sites; behaviour is parameterised by env vars:
  SITE_NAME  - display name (default "site")
  PEER_BASE  - base URL of the peer site, used for cross-site redirects

Endpoints:
  GET /                      index page
  GET /article/{n}           normal announcement (HTML title + body)
  GET /to-peer/{n}           302 -> peer's /article/{n}      (bidirectional jump)
  GET /chain/{k}             302 -> peer's /chain/{k-1}; /chain/0 renders an article
  GET /redirect-loop         302 -> peer's /redirect-loop    (endless ping-pong)
  GET /flaky/{n}?fails=&ra=  first `fails` requests -> 429 + Retry-After: ra, then article
  GET /busy                  always 503 + Retry-After: 1     (retry exhaustion)
  GET /flaky-no-ra           always 429 without Retry-After
  GET /slow/{n}?delay=       sleeps `delay` seconds, then article

Review (re-verification) test endpoints:
  GET /mutable/{n}           stateful article; POST /mutable/{n}/edit changes it,
                             POST /mutable/{n}/withdraw?status=404|410 withdraws it,
                             POST /mutable/{n}/blank makes it return an empty page
  GET /gated/{n}             first review request (X-Review-Id header) gets 429 +
                             Retry-After: 1, every later request gets the article;
                             used to make an earlier review deterministically slower
  GET /stats                 per-path request timestamps (test observability)
  POST /reset                clear stats, flaky counters and mutable/gate state
"""
import asyncio
import json
import os
import time
from collections import defaultdict

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

SITE_NAME = os.environ.get("SITE_NAME", "site")
PEER_BASE = os.environ.get("PEER_BASE", "http://127.0.0.1:8002")

app = FastAPI(title=f"mock tender site {SITE_NAME}")
_START = time.monotonic()
_HITS: dict[str, list[float]] = defaultdict(list)   # path -> request timestamps (s, relative)
_FLAKY: dict[str, int] = defaultdict(int)           # flaky key -> 429s already served
# mutable endpoint state: key -> {"mode": "article"|"withdrawn"|"blank", "status": int, "version": int}
_MUTABLE: dict[str, dict] = {}
_GATE_FIRED: set[str] = set()                       # gated paths already served their 429


@app.middleware("http")
async def record_hits(request, call_next):
    if request.url.path not in ("/stats", "/reset"):
        _HITS[request.url.path].append(round(time.monotonic() - _START, 3))
    return await call_next(request)


def _article_html(n: int, note: str = "") -> str:
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head><meta charset="utf-8"><title>招标公告 {n} 号 - {SITE_NAME} 政府采购网</title></head>
<body>
<header><h1>{SITE_NAME} 政府采购网</h1></header>
<article>
<h2>招标公告（编号：{SITE_NAME.upper()}-2026-{n:04d}）</h2>
<p>发布日期：2026-11-17</p>
<p>项目名称：{SITE_NAME} 第 {n} 号公共服务采购项目。</p>
<p>预算金额：人民币 {n}00 万元整，资金来源为财政拨款。</p>
<p>投标截止时间：2026-12-01 09:30（北京时间），逾期送达的投标文件恕不接受。</p>
<p>联系方式：{SITE_NAME} 采购中心公共资源交易科。{note}</p>
</article>
<script>console.log("tracker-{n}");</script>
<footer>版权所有 © {SITE_NAME} 采购中心</footer>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def index():
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>{SITE_NAME}</title></head>
<body><h1>{SITE_NAME} 政府采购网（模拟）</h1><ul>
<li><a href="/article/1">/article/1</a> 普通公告</li>
<li><a href="/to-peer/2">/to-peer/2</a> 跳转到对方站点公告</li>
<li><a href="/chain/3">/chain/3</a> 双向跳转链（3 跳）</li>
<li><a href="/flaky/1">/flaky/1</a> 限流后恢复（429 + Retry-After）</li>
<li><a href="/slow/1">/slow/1</a> 慢响应</li>
<li><a href="/mutable/1">/mutable/1</a> 可改写/撤回的公告（复核测试）</li>
<li><a href="/gated/1">/gated/1</a> 首次复核请求限流一次（复核交错测试）</li>
</ul></body></html>"""


@app.get("/article/{n}", response_class=HTMLResponse)
async def article(n: int):
    return _article_html(n)


@app.get("/to-peer/{n}")
async def to_peer(n: int):
    return RedirectResponse(url=f"{PEER_BASE}/article/{n}", status_code=302)


@app.get("/chain/{k}", response_class=HTMLResponse)
async def chain(k: int):
    if k <= 0:
        return _article_html(9999, note="（跳转链终点）")
    return RedirectResponse(url=f"{PEER_BASE}/chain/{k - 1}", status_code=302)


@app.get("/redirect-loop")
async def redirect_loop():
    return RedirectResponse(url=f"{PEER_BASE}/redirect-loop", status_code=302)


@app.get("/flaky/{n}", response_class=HTMLResponse)
async def flaky(n: int, fails: int = Query(2, ge=0), ra: int = Query(2, ge=0)):
    key = f"flaky:{n}"
    if _FLAKY[key] < fails:
        _FLAKY[key] += 1
        return JSONResponse({"detail": "rate limited, come back later"},
                            status_code=429, headers={"Retry-After": str(ra)})
    return _article_html(n, note="（限流后恢复）")


@app.get("/busy")
async def busy():
    return JSONResponse({"detail": "service unavailable"},
                        status_code=503, headers={"Retry-After": "1"})


@app.get("/flaky-no-ra")
async def flaky_no_ra():
    return JSONResponse({"detail": "rate limited without retry-after"}, status_code=429)


@app.get("/slow/{n}", response_class=HTMLResponse)
async def slow(n: int, delay: float = Query(3.0, ge=0.0, le=30.0)):
    await asyncio.sleep(delay)
    return _article_html(n, note=f"（慢响应 {delay}s）")


def _mutable_html(n: int, version: int) -> str:
    # Different per-version wording so an edit is visible to the extractor.
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head><meta charset="utf-8"><title>可变公告 {n} 号 v{version} - {SITE_NAME} 政府采购网</title></head>
<body>
<article>
<h2>招标公告（编号：{SITE_NAME.upper()}-MUT-{n:04d}，版本 {version}）</h2>
<p>项目名称：{SITE_NAME} 可变公告第 {n} 号，当前内容版本 {version}。</p>
<p>预算金额：人民币 {n + version}00 万元整。</p>
<p>版本说明：第 {version} 版正文，用于复核改写检测。</p>
</article>
</body>
</html>"""


def _mutable_state(key: str) -> dict:
    return _MUTABLE.setdefault(key, {"mode": "article", "status": 404, "version": 1})


@app.get("/mutable/{n}", response_class=HTMLResponse)
async def mutable_get(n: int):
    st = _mutable_state(f"mutable:{n}")
    if st["mode"] == "withdrawn":
        return JSONResponse({"detail": "announcement withdrawn"},
                            status_code=st["status"])
    if st["mode"] == "blank":
        return HTMLResponse("<!DOCTYPE html><html><head></head><body></body></html>")
    if st["mode"] == "rate_limited":
        return JSONResponse({"detail": "permanently rate limited (no retry-after)"},
                            status_code=429)
    return _mutable_html(n, st["version"])


@app.post("/mutable/{n}/edit")
async def mutable_edit(n: int):
    st = _mutable_state(f"mutable:{n}")
    st["mode"] = "article"
    st["version"] += 1
    return {"edited": True, "version": st["version"]}


@app.post("/mutable/{n}/withdraw")
async def mutable_withdraw(n: int, status: int = Query(404)):
    if status not in (404, 410):
        return JSONResponse({"detail": "status must be 404 or 410"}, status_code=422)
    st = _mutable_state(f"mutable:{n}")
    st["mode"] = "withdrawn"
    st["status"] = status
    return {"withdrawn": True, "status": status}


@app.post("/mutable/{n}/blank")
async def mutable_blank(n: int):
    st = _mutable_state(f"mutable:{n}")
    st["mode"] = "blank"
    return {"blank": True}


@app.post("/mutable/{n}/rate-limit")
async def mutable_rate_limit(n: int):
    """Switch the page to permanent 429 without Retry-After (review failure)."""
    st = _mutable_state(f"mutable:{n}")
    st["mode"] = "rate_limited"
    return {"rate_limited": True}


@app.get("/gated/{n}", response_class=HTMLResponse)
async def gated(n: int, request: Request):
    """First review request is rate limited once; everything else succeeds.

    Review requests carry X-Review-Id. The first such request to a gated
    path gets a 429 + Retry-After: 1 (and triggers the shared origin
    cooldown); retries and any later review get the article immediately.
    """
    key = f"gated:{n}"
    review_id = request.headers.get("x-review-id")
    if review_id and key not in _GATE_FIRED:
        _GATE_FIRED.add(key)
        return JSONResponse({"detail": "gated: first review attempt rate limited"},
                            status_code=429, headers={"Retry-After": "1"})
    return _article_html(n, note="（门控放行）")


@app.get("/hold/{n}", response_class=HTMLResponse)
async def hold(n: int, request: Request):
    """Deterministically slow review endpoint for interleaved-review tests.

    The first request carrying an X-Review-Id is answered 429 +
    Retry-After: 2 (shared origin cooldown). Every later request of THAT
    review id sleeps 2s before returning the article, while another review
    id gets the article immediately. This makes the earlier review
    deterministically slower end-to-end without relying on timing luck.
    """
    key = f"hold:{n}"
    review_id = request.headers.get("x-review-id")
    if not review_id:
        return _article_html(n)
    if key not in _GATE_FIRED:
        _GATE_FIRED.add(key)
        return JSONResponse({"detail": "hold: first review attempt rate limited"},
                            status_code=429, headers={"Retry-After": "2"})
    held_key = f"hold-slow:{review_id}:{n}"
    if held_key not in _GATE_FIRED:
        _GATE_FIRED.add(held_key)
        await asyncio.sleep(2.0)
    return _article_html(n, note="（延迟放行）")


@app.get("/stats")
async def stats():
    return {
        "site": SITE_NAME,
        "hits": {path: ts for path, ts in sorted(_HITS.items())},
        "mutable": json.loads(json.dumps(_MUTABLE)),
        "gate_fired": sorted(_GATE_FIRED),
    }


@app.post("/reset")
async def reset():
    _HITS.clear()
    _FLAKY.clear()
    _MUTABLE.clear()
    _GATE_FIRED.clear()
    return {"reset": True}
