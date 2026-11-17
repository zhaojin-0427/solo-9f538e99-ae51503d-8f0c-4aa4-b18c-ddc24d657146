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
"""

_UPDATABLE = {"current_url", "status", "hops", "attempts", "title", "content", "error"}


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
