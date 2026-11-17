"""Concurrent fetch engine.

Guarantees:
  * at most GLOBAL_CONCURRENCY in-flight HTTP requests across ALL jobs;
  * at most 1 in-flight request per origin (scheme://host:port);
  * redirects are re-queued against the *target* origin, max MAX_HOPS hops
    per announcement;
  * on HTTP 429/503 with an integer Retry-After, the whole origin is put
    into cooldown (persisted in SQLite): every unsent request to that
    origin, from every job, is delayed; the wait does NOT occupy a global
    slot and other origins keep fetching;
  * each URL is retried at most MAX_RETRIES times.
"""
import asyncio
import logging
import time
from urllib.parse import urljoin, urlparse

import httpx

log = logging.getLogger("collector.engine")

MAX_HOPS = 3
MAX_RETRIES = 2
GLOBAL_CONCURRENCY = 2
PUMP_IDLE_TIMEOUT = 30.0
MAX_BODY_BYTES = 10 * 1024 * 1024
REDIRECT_STATUSES = {301, 302, 303, 307, 308}
RETRYABLE_STATUSES = {429, 503}


def origin_of(url: str) -> str:
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"


def parse_retry_after(value):
    """Return integer seconds, or None if the header is absent/not a plain integer."""
    if value is None:
        return None
    value = value.strip()
    if value.isdigit():
        return int(value)
    return None


class FetchEngine:
    def __init__(self, db, extractor) -> None:
        self.db = db
        self.extractor = extractor
        self.global_sem = asyncio.Semaphore(GLOBAL_CONCURRENCY)
        self.origin_queues: dict[str, asyncio.Queue] = {}
        self.origin_pumps: dict[str, asyncio.Task] = {}
        self.cooldowns: dict[str, float] = {}  # origin -> unix ts until which requests are delayed
        self.client: httpx.AsyncClient | None = None
        self._stopping = False

    async def start(self) -> None:
        self.client = httpx.AsyncClient(
            follow_redirects=False,  # redirects are followed manually, queued per target origin
            timeout=httpx.Timeout(30.0, connect=10.0),
            headers={"User-Agent": "tender-corpus-collector/1.0"},
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
        )
        now = time.time()
        for row in self.db.active_cooldowns(now):
            self.cooldowns[row["origin"]] = row["until_ts"]
        if self.cooldowns:
            log.info("restored %d active cooldown(s) from database", len(self.cooldowns))

    async def stop(self) -> None:
        self._stopping = True
        pumps = list(self.origin_pumps.values())
        for task in pumps:
            task.cancel()
        if pumps:
            await asyncio.gather(*pumps, return_exceptions=True)
        if self.client is not None:
            await self.client.aclose()

    def cooldowns_snapshot(self) -> dict[str, float]:
        return dict(self.cooldowns)

    # ------------------------------------------------------------------
    def submit(self, ann_id: int, url: str, hops: int, attempts: int) -> None:
        """Enqueue a fetch task against the queue of its (target) origin."""
        origin = origin_of(url)
        queue = self.origin_queues.get(origin)
        if queue is None:
            queue = self.origin_queues[origin] = asyncio.Queue()
        queue.put_nowait((ann_id, url, hops, attempts))
        pump = self.origin_pumps.get(origin)
        if pump is None or pump.done():
            self.origin_pumps[origin] = asyncio.create_task(self._pump(origin))

    async def _pump(self, origin: str) -> None:
        """Serial worker for one origin: guarantees <=1 in-flight request per origin."""
        queue = self.origin_queues[origin]
        try:
            while not self._stopping:
                try:
                    ann_id, url, hops, attempts = await asyncio.wait_for(
                        queue.get(), timeout=PUMP_IDLE_TIMEOUT
                    )
                except asyncio.TimeoutError:
                    return  # idle: exit; submit() recreates the pump on demand
                # Cooldown wait happens BEFORE taking a global slot, so a
                # delayed origin never blocks the global quota nor other origins.
                delay = self.cooldowns.get(origin, 0.0) - time.time()
                if delay > 0:
                    log.info("origin %s in cooldown, task ann=%s delayed %.1fs", origin, ann_id, delay)
                    await asyncio.sleep(delay)
                try:
                    await self._fetch_one(ann_id, url, hops, attempts)
                except Exception:  # pragma: no cover - defensive
                    log.exception("unexpected engine error on ann=%s", ann_id)
                    self.db.update_ann(ann_id, status="failed", error="internal engine error")
        finally:
            self.origin_pumps.pop(origin, None)
            if not queue.empty() and not self._stopping:
                self.origin_pumps[origin] = asyncio.create_task(self._pump(origin))

    async def _fetch_one(self, ann_id: int, url: str, hops: int, attempts: int) -> None:
        self.db.update_ann(ann_id, status="fetching", current_url=url, hops=hops, attempts=attempts)
        log.info("GET %s (ann=%s hop=%d attempt=%d)", url, ann_id, hops, attempts)
        async with self.global_sem:  # global quota: max 2 in-flight requests
            try:
                resp = await self.client.get(url)
            except httpx.HTTPError as exc:
                log.warning("ann=%s network error: %s", ann_id, exc)
                self._retry_or_fail(ann_id, url, hops, attempts, f"network error: {type(exc).__name__}")
                return

        status = resp.status_code

        if status in RETRYABLE_STATUSES:
            retry_after = parse_retry_after(resp.headers.get("retry-after"))
            if retry_after is not None:
                origin = origin_of(url)
                until = time.time() + retry_after
                if until > self.cooldowns.get(origin, 0.0):
                    self.cooldowns[origin] = until
                    self.db.set_cooldown(origin, until)  # persisted: survives restarts
                log.info("origin %s backs off for %ds (HTTP %d)", origin, retry_after, status)
            reason = f"HTTP {status}" + (f" (Retry-After {retry_after}s)" if retry_after is not None else "")
            self._retry_or_fail(ann_id, url, hops, attempts, reason)
            return

        if status in REDIRECT_STATUSES:
            location = resp.headers.get("location")
            if not location:
                self.db.update_ann(ann_id, status="failed", error=f"HTTP {status} redirect without Location")
                return
            if hops >= MAX_HOPS:
                self.db.update_ann(ann_id, status="failed", error=f"too many redirects (limit {MAX_HOPS} hops)")
                log.warning("ann=%s exceeded %d hops at %s", ann_id, MAX_HOPS, url)
                return
            target = urljoin(url, location)
            log.info("ann=%s hop %d: %s -> %s", ann_id, hops + 1, url, target)
            # fresh retry budget for the new URL; queued against the TARGET origin
            self.db.update_ann(ann_id, status="pending", current_url=target, hops=hops + 1, attempts=0, error=None)
            self.submit(ann_id, target, hops + 1, 0)
            return

        if 200 <= status < 300:
            if len(resp.content) > MAX_BODY_BYTES:
                self.db.update_ann(ann_id, status="failed", error="response body too large")
                return
            title, content = self.extractor(resp.text)
            if not title and not content:
                self.db.update_ann(ann_id, status="failed", error="no extractable title/content")
                return
            self.db.update_ann(ann_id, status="success", title=title, content=content, error=None)
            log.info("ann=%s success: %r (%d chars, %d hops, %d retries)",
                     ann_id, title[:60], len(content), hops, attempts)
            return

        self.db.update_ann(ann_id, status="failed", error=f"HTTP {status}")
        log.warning("ann=%s failed with HTTP %d", ann_id, status)

    def _retry_or_fail(self, ann_id: int, url: str, hops: int, attempts: int, reason: str) -> None:
        if attempts < MAX_RETRIES:
            log.info("ann=%s retry %d/%d for %s (%s)", ann_id, attempts + 1, MAX_RETRIES, url, reason)
            self.db.update_ann(ann_id, status="pending", attempts=attempts + 1, error=reason)
            self.submit(ann_id, url, hops, attempts + 1)
        else:
            self.db.update_ann(ann_id, status="failed",
                               error=f"{reason}; retries exhausted ({MAX_RETRIES})")
            log.warning("ann=%s permanently failed: %s", ann_id, reason)
