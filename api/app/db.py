"""SQLite persistence layer.

Stores jobs, per-announcement fetch state and per-origin cooldowns so that a
restart can resume unfinished work without re-fetching successful documents
and without losing active cooldown deadlines.
"""
import sqlite3
import threading
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id         TEXT PRIMARY KEY,
    created_at REAL NOT NULL,
    total      INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS announcements (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id      TEXT NOT NULL REFERENCES jobs(id),
    url         TEXT NOT NULL,           -- originally submitted URL
    current_url TEXT NOT NULL,           -- URL currently being fetched (changes on redirect)
    status      TEXT NOT NULL DEFAULT 'pending',  -- pending|fetching|success|failed
    hops        INTEGER NOT NULL DEFAULT 0,       -- redirects followed so far
    attempts    INTEGER NOT NULL DEFAULT 0,       -- retries used on current_url
    title       TEXT,
    content     TEXT,
    error       TEXT,
    updated_at  REAL NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_ann_job ON announcements(job_id);
CREATE TABLE IF NOT EXISTS cooldowns (
    origin   TEXT PRIMARY KEY,
    until_ts REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS reviews (
    id         TEXT PRIMARY KEY,
    job_id     TEXT NOT NULL REFERENCES jobs(id),
    seq        INTEGER NOT NULL,          -- per-job monotonic ordering number
    status     TEXT NOT NULL DEFAULT 'running',  -- running|completed|superseded
    total      INTEGER NOT NULL,
    created_at REAL NOT NULL,
    finished_at REAL
);
CREATE INDEX IF NOT EXISTS idx_reviews_job ON reviews(job_id);
CREATE TABLE IF NOT EXISTS review_items (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    review_id   TEXT NOT NULL REFERENCES reviews(id),
    ann_id      INTEGER NOT NULL REFERENCES announcements(id),
    url         TEXT NOT NULL,            -- original URL, re-fetched from scratch
    current_url TEXT NOT NULL,            -- URL currently being fetched (redirects)
    status      TEXT NOT NULL DEFAULT 'pending',  -- pending|fetching|unchanged|modified|withdrawn|error
    hops        INTEGER NOT NULL DEFAULT 0,
    attempts    INTEGER NOT NULL DEFAULT 0,
    title       TEXT,                     -- freshly extracted title (if any)
    content     TEXT,                     -- freshly extracted body (if any)
    changes     TEXT,                     -- JSON change description for modified items
    error       TEXT,
    updated_at  REAL NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_review_items_review ON review_items(review_id);
CREATE TABLE IF NOT EXISTS review_reports (
    job_id        TEXT PRIMARY KEY REFERENCES jobs(id),
    review_id     TEXT NOT NULL,          -- latest published review for the job
    seq           INTEGER NOT NULL,       -- its ordering number (monotonic per job)
    published_at  REAL NOT NULL,
    payload       TEXT NOT NULL           -- immutable JSON snapshot of the report
);
"""

_UPDATABLE = {"current_url", "status", "hops", "attempts", "title", "content", "error"}
_UPDATABLE_REVIEW_ITEM = {
    "current_url", "status", "hops", "attempts", "title", "content", "changes", "error",
}


class DB:
    def __init__(self, path: str) -> None:
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._lock = threading.Lock()
        with self._lock, self._conn:
            self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---- jobs ---------------------------------------------------------
    def create_job(self, job_id: str, total: int) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO jobs (id, created_at, total) VALUES (?, ?, ?)",
                (job_id, time.time(), total),
            )

    def get_job(self, job_id: str):
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()

    def list_jobs(self):
        with self._lock:
            return self._conn.execute(
                """
                SELECT j.id, j.created_at, j.total,
                       COALESCE(SUM(a.status = 'success'), 0)  AS success,
                       COALESCE(SUM(a.status = 'failed'), 0)   AS failed,
                       COALESCE(SUM(a.status IN ('pending','fetching')), 0) AS unfinished
                FROM jobs j LEFT JOIN announcements a ON a.job_id = j.id
                GROUP BY j.id ORDER BY j.created_at DESC
                """
            ).fetchall()

    # ---- announcements -------------------------------------------------
    def add_announcement(self, job_id: str, url: str) -> int:
        with self._lock, self._conn:
            cur = self._conn.execute(
                "INSERT INTO announcements (job_id, url, current_url, status, updated_at)"
                " VALUES (?, ?, ?, 'pending', ?)",
                (job_id, url, url, time.time()),
            )
            return cur.lastrowid

    def update_ann(self, ann_id: int, **fields) -> None:
        fields = {k: v for k, v in fields.items() if k in _UPDATABLE}
        fields["updated_at"] = time.time()
        cols = ", ".join(f"{k} = ?" for k in fields)
        with self._lock, self._conn:
            self._conn.execute(
                f"UPDATE announcements SET {cols} WHERE id = ?",
                (*fields.values(), ann_id),
            )

    def list_announcements(self, job_id: str):
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM announcements WHERE job_id = ? ORDER BY id", (job_id,)
            ).fetchall()

    def unfinished_announcements(self):
        with self._lock:
            return self._conn.execute(
                "SELECT id, current_url, hops, attempts FROM announcements"
                " WHERE status IN ('pending', 'fetching') ORDER BY id"
            ).fetchall()

    # ---- cooldowns ------------------------------------------------------
    def set_cooldown(self, origin: str, until_ts: float) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO cooldowns (origin, until_ts) VALUES (?, ?)"
                " ON CONFLICT(origin) DO UPDATE SET until_ts = excluded.until_ts",
                (origin, until_ts),
            )

    def active_cooldowns(self, now: float):
        with self._lock:
            return self._conn.execute(
                "SELECT origin, until_ts FROM cooldowns WHERE until_ts > ?", (now,)
            ).fetchall()

    # ---- reviews --------------------------------------------------------
    def next_review_seq(self, job_id: str) -> int:
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT COALESCE(MAX(seq), 0) AS m FROM reviews WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            return row["m"] + 1

    def create_review(self, review_id: str, job_id: str, seq: int,
                      items: list[tuple[int, str]]) -> None:
        """Insert a review and its items in one transaction.

        items: list of (announcement_id, original_url).
        """
        now = time.time()
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO reviews (id, job_id, seq, status, total, created_at)"
                " VALUES (?, ?, ?, 'running', ?, ?)",
                (review_id, job_id, seq, len(items), now),
            )
            self._conn.executemany(
                "INSERT INTO review_items (review_id, ann_id, url, current_url, updated_at)"
                " VALUES (?, ?, ?, ?, ?)",
                [(review_id, ann_id, url, url, now) for ann_id, url in items],
            )

    def get_review(self, review_id: str):
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM reviews WHERE id = ?", (review_id,)
            ).fetchone()

    def list_reviews(self, job_id: str):
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM reviews WHERE job_id = ? ORDER BY seq", (job_id,)
            ).fetchall()

    def list_review_items(self, review_id: str):
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM review_items WHERE review_id = ? ORDER BY id", (review_id,)
            ).fetchall()

    def get_review_item(self, item_id: int):
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM review_items WHERE id = ?", (item_id,)
            ).fetchone()

    def update_review_item(self, item_id: int, **fields) -> None:
        fields = {k: v for k, v in fields.items() if k in _UPDATABLE_REVIEW_ITEM}
        fields["updated_at"] = time.time()
        cols = ", ".join(f"{k} = ?" for k in fields)
        with self._lock, self._conn:
            self._conn.execute(
                f"UPDATE review_items SET {cols} WHERE id = ?",
                (*fields.values(), item_id),
            )

    def review_items_unfinished(self, review_id: str):
        with self._lock:
            return self._conn.execute(
                "SELECT id, current_url, hops, attempts FROM review_items"
                " WHERE review_id = ? AND status IN ('pending', 'fetching') ORDER BY id",
                (review_id,),
            ).fetchall()

    def unfinished_reviews(self):
        """Reviews that never reached a terminal state (e.g. after a crash)."""
        with self._lock:
            return self._conn.execute(
                "SELECT id FROM reviews WHERE status = 'running' ORDER BY created_at"
            ).fetchall()

    def review_all_terminal(self, review_id: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM review_items"
                " WHERE review_id = ? AND status IN ('pending', 'fetching')",
                (review_id,),
            ).fetchone()
            return row["n"] == 0

    def publish_review_if_newest(self, review_id: str, report_payload: str) -> bool:
        """Atomically publish a completed review.

        Returns True when this review became the current report for its job,
        False when a newer review had already claimed the slot (this one is
        then marked 'superseded'). A single transaction makes the check and
        the swap atomic, so an older, slower review can never overwrite a
        newer, already-published report.
        """
        now = time.time()
        with self._lock, self._conn:
            review = self._conn.execute(
                "SELECT * FROM reviews WHERE id = ?", (review_id,)
            ).fetchone()
            if review is None:
                return False
            current = self._conn.execute(
                "SELECT seq FROM review_reports WHERE job_id = ?",
                (review["job_id"],),
            ).fetchone()
            if current is not None and current["seq"] >= review["seq"]:
                self._conn.execute(
                    "UPDATE reviews SET status = 'superseded', finished_at = ? WHERE id = ?",
                    (now, review_id),
                )
                return False
            self._conn.execute(
                "INSERT INTO review_reports (job_id, review_id, seq, published_at, payload)"
                " VALUES (?, ?, ?, ?, ?)"
                " ON CONFLICT(job_id) DO UPDATE SET"
                " review_id = excluded.review_id, seq = excluded.seq,"
                " published_at = excluded.published_at, payload = excluded.payload",
                (review["job_id"], review_id, review["seq"], now, report_payload),
            )
            self._conn.execute(
                "UPDATE reviews SET status = 'completed', finished_at = ? WHERE id = ?",
                (now, review_id),
            )
            return True

    def get_report(self, job_id: str):
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM review_reports WHERE job_id = ?", (job_id,)
            ).fetchone()

    def get_announcement(self, ann_id: int):
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM announcements WHERE id = ?", (ann_id,)
            ).fetchone()
