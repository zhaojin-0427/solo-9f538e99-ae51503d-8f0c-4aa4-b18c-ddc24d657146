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
  GET /stats                 per-path request timestamps (test observability)
  POST /reset                clear stats and flaky counters
"""
import asyncio
import os
import time
from collections import defaultdict

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

SITE_NAME = os.environ.get("SITE_NAME", "site")
PEER_BASE = os.environ.get("PEER_BASE", "http://127.0.0.1:8002")

app = FastAPI(title=f"mock tender site {SITE_NAME}")
_START = time.monotonic()
_HITS: dict[str, list[float]] = defaultdict(list)   # path -> request timestamps (s, relative)
_FLAKY: dict[str, int] = defaultdict(int)           # flaky key -> 429s already served


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


@app.get("/stats")
async def stats():
    return {"site": SITE_NAME, "hits": {path: ts for path, ts in sorted(_HITS.items())}}


@app.post("/reset")
async def reset():
    _HITS.clear()
    _FLAKY.clear()
    return {"reset": True}
