"""Concurrent fetch engine.

Guarantees:
  * at most GLOBAL_CONCURRENCY in-flight HTTP requests across ALL jobs AND
    all review runs;
  * at most 1 in-flight request per origin (scheme://host:port);
  * redirects are re-queued against the *target* origin, max MAX_HOPS hops
    per task;
  * on HTTP 429/503 with an integer Retry-After, the whole origin is put
    into cooldown (persisted in SQLite): every unsent request to that
    origin, from every job/review, is delayed; the wait does NOT occupy a
    global slot and other origins keep fetching;
  * each URL is retried at most MAX_RETRIES times.

Two kinds of tasks flow through the exact same queues/quotas:
  * "collect" tasks write the announcements table of ordinary jobs;
  * "review"  tasks re-fetch an already-collected announcement from its
    ORIGINAL URL and write independent review_items. They never mutate the
    original corpus. Only a 404/410 verdict means "withdrawn"; rate-limit,
    network and parsing failures are recorded as inconclusive "error"
    items and cannot change the previously published report.
"""
import asyncio
import json
import logging
import time
from urllib.parse import urljoin, urlparse

import httpx

from .diffing import compare_documents

log = logging.getLogger("collector.engine")

MAX_HOPS = 3
MAX_RETRIES = 2
GLOBAL_CONCURRENCY = 2
PUMP_IDLE_TIMEOUT = 30.0
MAX_BODY_BYTES = 10 * 1024 * 1024
REDIRECT_STATUSES = {301, 302, 303, 307, 308}
RETRYABLE_STATUSES = {429, 503}
WITHDRAWN_STATUSES = {404, 410}

COLLECT = "collect"
REVIEW = "review"
REVIEW_TERMINAL = {"unchanged", "modified", "withdrawn", "error"}


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
    def __init__(self, db, extractor, review_completed_cb=None) -> None:
        self.db = db
        self.extractor = extractor
        # Called with a review_id once every item of that review is terminal.
        self.review_completed_cb = review_completed_cb
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
    def submit(self, kind: str, ref_id: int, url: str, hops: int, attempts: int,
               review_id: str | None = None) -> None:
        """Enqueue a fetch task against the queue of its (target) origin.

        kind is COLLECT (ref_id = announcements.id) or REVIEW
        (ref_id = review_items.id, review_id identifies the parent run).
        """
        origin = origin_of(url)
        queue = self.origin_queues.get(origin)
        if queue is None:
            queue = self.origin_queues[origin] = asyncio.Queue()
        queue.put_nowait((kind, ref_id, url, hops, attempts, review_id))
        pump = self.origin_pumps.get(origin)
        if pump is None or pump.done():
            self.origin_pumps[origin] = asyncio.create_task(self._pump(origin))

    async def _pump(self, origin: str) -> None:
        """Serial worker for one origin: guarantees <=1 in-flight request per origin."""
        queue = self.origin_queues[origin]
        try:
            while not self._stopping:
                try:
                    kind, ref_id, url, hops, attempts, review_id = await asyncio.wait_for(
                        queue.get(), timeout=PUMP_IDLE_TIMEOUT
                    )
                except asyncio.TimeoutError:
                    return  # idle: exit; submit() recreates the pump on demand
                # Cooldown wait happens BEFORE taking a global slot, so a
                # delayed origin never blocks the global quota nor other origins.
                delay = self.cooldowns.get(origin, 0.0) - time.time()
                if delay > 0:
                    log.info("origin %s in cooldown, task %s=%s delayed %.1fs",
                             origin, kind, ref_id, delay)
                    await asyncio.sleep(delay)
                try:
                    await self._fetch_one(kind, ref_id, url, hops, attempts, review_id)
                except Exception:  # pragma: no cover - defensive
                    log.exception("unexpected engine error on %s=%s", kind, ref_id)
                    self._terminal_failure(
                        kind, ref_id, review_id, "internal engine error", url, hops, attempts)
        finally:
            self.origin_pumps.pop(origin, None)
            if not queue.empty() and not self._stopping:
                self.origin_pumps[origin] = asyncio.create_task(self._pump(origin))

    async def _fetch_one(self, kind: str, ref_id: int, url: str,
                         hops: int, attempts: int, review_id: str | None) -> None:
        if kind == REVIEW:
            self.db.update_review_item(
                ref_id, status="fetching", current_url=url, hops=hops, attempts=attempts)
        else:
            self.db.update_ann(
                ref_id, status="fetching", current_url=url, hops=hops, attempts=attempts)
        log.info("GET %s (%s=%s hop=%d attempt=%d)", url, kind, ref_id, hops, attempts)
        headers = {"X-Review-Id": review_id} if kind == REVIEW and review_id else None
        async with self.global_sem:  # global quota: max 2 in-flight requests
            try:
                resp = await self.client.get(url, headers=headers)
            except httpx.HTTPError as exc:
                log.warning("%s=%s network error: %s", kind, ref_id, exc)
                self._retry_or_fail(
                    kind, ref_id, url, hops, attempts, review_id,
                    f"network error: {type(exc).__name__}")
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
            self._retry_or_fail(kind, ref_id, url, hops, attempts, review_id, reason)
            return

        if status in REDIRECT_STATUSES:
            location = resp.headers.get("location")
            if not location:
                self._terminal_failure(
                    kind, ref_id, review_id,
                    f"HTTP {status} redirect without Location", url, hops, attempts)
                return
            if hops >= MAX_HOPS:
                self._terminal_failure(
                    kind, ref_id, review_id,
                    f"too many redirects (limit {MAX_HOPS} hops)", url, hops, attempts)
                log.warning("%s=%s exceeded %d hops at %s", kind, ref_id, MAX_HOPS, url)
                return
            target = urljoin(url, location)
            log.info("%s=%s hop %d: %s -> %s", kind, ref_id, hops + 1, url, target)
            # fresh retry budget for the new URL; queued against the TARGET origin
            if kind == REVIEW:
                self.db.update_review_item(
                    ref_id, status="pending", current_url=target,
                    hops=hops + 1, attempts=0, error=None)
            else:
                self.db.update_ann(
                    ref_id, status="pending", current_url=target,
                    hops=hops + 1, attempts=0, error=None)
            self.submit(kind, ref_id, target, hops + 1, 0, review_id)
            return

        if kind == REVIEW and status in WITHDRAWN_STATUSES:
            # Only 404/410 constitute a withdrawal.
            self.db.update_review_item(
                ref_id, status="withdrawn", current_url=url,
                error=f"HTTP {status}")
            log.info("review item %s withdrawn at %s (HTTP %d)", ref_id, url, status)
            self._after_review_terminal(review_id)
            return

        if 200 <= status < 300:
            if len(resp.content) > MAX_BODY_BYTES:
                self._terminal_failure(
                    kind, ref_id, review_id, "response body too large", url, hops, attempts)
                return
            title, content = self.extractor(resp.text)
            if not title and not content:
                self._terminal_failure(
                    kind, ref_id, review_id,
                    "no extractable title/content", url, hops, attempts)
                return
            if kind == REVIEW:
                self._finish_review_item(ref_id, review_id, url, title, content)
                return
            self.db.update_ann(ref_id, status="success", title=title, content=content, error=None)
            log.info("ann=%s success: %r (%d chars, %d hops, %d retries)",
                     ref_id, title[:60], len(content), hops, attempts)
            return

        # Any other status (e.g. 403/500): for collection it is a hard
        # failure; for a review it is an inconclusive error that must not
        # change the old report.
        self._terminal_failure(
            kind, ref_id, review_id, f"HTTP {status}", url, hops, attempts)
        log.warning("%s=%s terminal: HTTP %d", kind, ref_id, status)

    # ------------------------------------------------------------------
    def _retry_or_fail(self, kind, ref_id, url, hops, attempts, review_id, reason) -> None:
        if attempts < MAX_RETRIES:
            log.info("%s=%s retry %d/%d for %s (%s)",
                     kind, ref_id, attempts + 1, MAX_RETRIES, url, reason)
            if kind == REVIEW:
                self.db.update_review_item(
                    ref_id, status="pending", attempts=attempts + 1, error=reason)
            else:
                self.db.update_ann(
                    ref_id, status="pending", attempts=attempts + 1, error=reason)
            self.submit(kind, ref_id, url, hops, attempts + 1, review_id)
        else:
            self._terminal_failure(
                kind, ref_id, review_id,
                f"{reason}; retries exhausted ({MAX_RETRIES})", url, hops, attempts)
            log.warning("%s=%s inconclusive/failed: %s", kind, ref_id, reason)

    def _terminal_failure(self, kind, ref_id, review_id, reason, url, hops, attempts) -> None:
        """Persist a terminal non-success state for one task."""
        if kind == REVIEW:
            # Rate limiting, network trouble, redirect-limit and parsing
            # problems are "error": they never produce a withdrawal/change
            # verdict and therefore cannot alter the previous report.
            self.db.update_review_item(
                ref_id, status="error", current_url=url,
                hops=hops, attempts=attempts, error=reason)
            self._after_review_terminal(review_id)
        else:
            self.db.update_ann(
                ref_id, status="failed", current_url=url,
                hops=hops, attempts=attempts, error=reason)

    def _finish_review_item(self, item_id: int, review_id: str,
                            final_url: str, title: str, content: str) -> None:
        item = self.db.get_review_item(item_id)
        original = self.db.get_announcement(item["ann_id"])
        result = compare_documents(
            original["title"] or "", original["content"] or "", title, content)
        self.db.update_review_item(
            item_id, status=result["verdict"], current_url=final_url,
            title=title, content=content,
            changes=json.dumps(result["changes"], ensure_ascii=False)
            if result["changes"] is not None else None,
            error=None)
        log.info("review item %s verdict=%s (%s)", item_id, result["verdict"], final_url)
        self._after_review_terminal(review_id)

    def _after_review_terminal(self, review_id: str | None) -> None:
        if not review_id:
            return
        if self.db.review_all_terminal(review_id):
            log.info("review %s: all items terminal, publishing decision", review_id)
            if self.review_completed_cb is not None:
                try:
                    self.review_completed_cb(review_id)
                except Exception:  # pragma: no cover - defensive
                    log.exception("review %s publication callback failed", review_id)
