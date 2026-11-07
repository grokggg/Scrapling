"""Lease-based, crash-safe URL frontier shared by any number of workers.

Why this exists
---------------
Scrapling's built-in scheduler keeps the queue in memory and persists it as a
single pickle. That is fine for one process; it cannot survive two crawlers
sharing a run, and a SIGKILL loses everything since the last checkpoint.

This frontier is a small SQLite(WAL) state machine instead:

    pending --claim--> leased --complete--> done
       ^                  |
       |                  +-- lease expiry / failure --> pending (attempts+1)
       +-- attempts > max_attempts --> failed

Guarantees:
* **At-least-once**: a claimed row stays visible to the run until its lease
  expires or it is completed. Two workers can never hold the same row.
* **Ordered by priority**, then FIFO within the same priority.
* **Idempotent inserts**: the same canonical URL never enters twice.
* **Crash-safe**: every transition is a single `BEGIN IMMEDIATE` transaction.
"""

from __future__ import annotations

import random
import sqlite3
import threading
import time
from dataclasses import dataclass
from hashlib import sha1
from pathlib import Path
from typing import Iterable, Iterator, Literal, Optional
from urllib.parse import urldefrag, urlsplit, urlunsplit

State = Literal["pending", "leased", "done", "failed"]
MASK64 = (1 << 64) - 1  # SimHash is unsigned 64-bit; SQLite INTEGER is signed, so store hex

_SCHEMA = """
CREATE TABLE IF NOT EXISTS urls (
    url_hash     TEXT PRIMARY KEY,
    url          TEXT NOT NULL,
    domain       TEXT NOT NULL,
    priority     INTEGER NOT NULL DEFAULT 0,
    depth        INTEGER NOT NULL DEFAULT 0,
    state        TEXT NOT NULL DEFAULT 'pending',
    attempts     INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,
    next_at      REAL NOT NULL,
    lease_until  REAL,
    worker       TEXT,
    simhash      TEXT,
    content_hash TEXT,
    error        TEXT,
    created_at   REAL NOT NULL,
    updated_at   REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_claim ON urls(state, next_at, priority DESC, created_at);
CREATE INDEX IF NOT EXISTS idx_domain ON urls(domain, state);
CREATE TABLE IF NOT EXISTS links (
    src TEXT NOT NULL,
    dst TEXT NOT NULL,
    PRIMARY KEY (src, dst)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS kv (
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL
);
"""

_TRACKING_QUERY_KEYS_PREFIXES = ("utm_", "spm", "fbclid", "gclid", "share_", "from_")


def canonical_url(url: str, *, keep_fragments: bool = False) -> str:
    """Normalise a URL so trivially different spellings dedupe to one row.

    Lowercases scheme/host, drops default ports, sorts query parameters and
    strips tracking parameters. Does **not** rewrite paths (case matters there).
    """
    if not keep_fragments:
        url, _ = urldefrag(url)
    parts = urlsplit(url.strip())
    scheme = parts.scheme.lower()
    host = (parts.hostname or "").lower()
    port = parts.port
    if port and not ((scheme == "http" and port == 80) or (scheme == "https" and port == 443)):
        host = f"{host}:{port}"
    query_pairs = []
    for pair in parts.query.split("&"):
        if not pair:
            continue
        key = pair.split("=", 1)[0]
        if any(key.lower().startswith(p) for p in _TRACKING_QUERY_KEYS_PREFIXES):
            continue
        query_pairs.append(pair)
    path = parts.path or "/"
    return urlunsplit((scheme, host, path, "&".join(sorted(query_pairs)), ""))


def url_key(url: str, *, keep_fragments: bool = False) -> str:
    return sha1(canonical_url(url, keep_fragments=keep_fragments).encode("utf-8")).hexdigest()


def domain_of(url: str) -> str:
    return (urlsplit(url).hostname or "").lower()


@dataclass(frozen=True)
class UrlRecord:
    url_hash: str
    url: str
    domain: str
    priority: int
    depth: int
    attempts: int
    state: str


@dataclass(frozen=True)
class FrontierStats:
    pending: int
    leased: int
    done: int
    failed: int
    total: int
    domains: int

    @property
    def progress(self) -> float:
        return 0.0 if not self.total else round(100.0 * self.done / self.total, 2)


class Frontier:
    """SQLite-backed URL frontier. Safe to share across threads *and* processes."""

    def __init__(self, path: str | Path = "crawl.db", *, max_attempts: int = 5, backoff_base: float = 2.0, backoff_cap: float = 900.0):
        self.path = str(path)
        self.max_attempts = max_attempts
        self.backoff_base = backoff_base
        self.backoff_cap = backoff_cap
        self._local = threading.local()
        self._lock = threading.Lock()
        self._init_schema()

    # ---------------------------------------------------------------- plumbing
    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(self.path, timeout=30.0, isolation_level=None)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=30000")
            self._local.conn = conn
        return conn

    def _init_schema(self) -> None:
        with self._lock:
            conn = self._conn()
            conn.executescript(_SCHEMA)

    @property
    def connection(self) -> sqlite3.Connection:
        return self._conn()

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    # ------------------------------------------------------------------ writes
    def add(
        self,
        url: str,
        *,
        priority: int = 0,
        depth: int = 0,
        parent: Optional[str] = None,
        keep_fragments: bool = False,
    ) -> bool:
        """Insert a URL. Returns False when it was already known (dedupe)."""
        key = url_key(url, keep_fragments=keep_fragments)
        now = time.time()
        conn = self._conn()
        cur = conn.execute(
            """INSERT OR IGNORE INTO urls
               (url_hash, url, domain, priority, depth, state, next_at, created_at, updated_at)
               VALUES (?,?,?,?,?, 'pending', ?, ?, ?)""",
            (key, url, domain_of(url), priority, depth, now, now, now),
        )
        added = cur.rowcount == 1
        if added and parent:
            parent_key = url_key(parent, keep_fragments=keep_fragments)
            conn.execute("INSERT OR IGNORE INTO links (src, dst) VALUES (?,?)", (parent_key, key))
        return added

    def extend(self, urls: Iterable[str], *, priority: int = 0, depth: int = 0, parent: Optional[str] = None) -> int:
        return sum(self.add(u, priority=priority, depth=depth, parent=parent) for u in urls)

    def claim(self, worker: str, *, limit: int = 1, lease: float = 120.0) -> list[UrlRecord]:
        """Atomically lease up to `limit` ready URLs for `worker`."""
        if limit < 1:
            return []
        now = time.time()
        conn = self._conn()
        with self._lock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                rows = conn.execute(
                    """SELECT url_hash, url, domain, priority, depth, attempts
                       FROM urls
                       WHERE state = 'pending' AND next_at <= ?
                       ORDER BY priority DESC, created_at ASC
                       LIMIT ?""",
                    (now, limit),
                ).fetchall()
                for row in rows:
                    conn.execute(
                        """UPDATE urls SET state='leased', worker=?, lease_until=?, updated_at=?
                           WHERE url_hash=? AND state='pending'""",
                        (worker, now + lease, now, row["url_hash"]),
                    )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return [
            UrlRecord(r["url_hash"], r["url"], r["domain"], r["priority"], r["depth"], r["attempts"], "leased")
            for r in rows
        ]

    def complete(
        self,
        url_hash: str,
        *,
        ok: bool,
        simhash: Optional[int] = None,
        content_hash: Optional[str] = None,
        error: Optional[str] = None,
        retry_after: Optional[float] = None,
    ) -> str:
        """Finish a leased URL. Returns the new state."""
        now = time.time()
        conn = self._conn()
        row = conn.execute("SELECT attempts, max_attempts FROM urls WHERE url_hash=?", (url_hash,)).fetchone()
        if row is None:
            return "missing"
        if ok:
            conn.execute(
                """UPDATE urls SET state='done', simhash=?, content_hash=?, error=NULL,
                   lease_until=NULL, worker=NULL, updated_at=? WHERE url_hash=?""",
                (None if simhash is None else f"{simhash & MASK64:016x}", content_hash, now, url_hash),
            )
            return "done"

        attempts = row["attempts"] + 1
        if attempts >= row["max_attempts"]:
            conn.execute(
                """UPDATE urls SET state='failed', attempts=?, error=?, lease_until=NULL,
                   worker=NULL, updated_at=? WHERE url_hash=?""",
                (attempts, (error or "")[:500], now, url_hash),
            )
            return "failed"

        delay = retry_after if retry_after is not None else min(self.backoff_base ** attempts, self.backoff_cap)
        delay *= 1 + random.random() * 0.25  # jitter: avoid retry stampedes
        conn.execute(
            """UPDATE urls SET state='pending', attempts=?, error=?, next_at=?, lease_until=NULL,
               worker=NULL, updated_at=? WHERE url_hash=?""",
            (attempts, (error or "")[:500], now + delay, now, url_hash),
        )
        return "pending"

    def release(self, url_hash: str) -> None:
        """Return a leased URL to the queue without counting an attempt."""
        now = time.time()
        self._conn().execute(
            "UPDATE urls SET state='pending', lease_until=NULL, worker=NULL, updated_at=? WHERE url_hash=? AND state='leased'",
            (now, url_hash),
        )

    def skip(self, url_hash: str, reason: str) -> None:
        """Mark a URL as processed-but-not-fetched (out of scope, duplicate, ...)."""
        self._conn().execute(
            """UPDATE urls SET state='done', error=?, lease_until=NULL, worker=NULL, updated_at=?
               WHERE url_hash=?""",
            (f"skipped:{reason}"[:200], time.time(), url_hash),
        )

    def reap(self) -> int:
        """Recover rows whose worker died mid-flight. Returns how many were re-queued."""
        now = time.time()
        conn = self._conn()
        with self._lock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                rows = conn.execute(
                    "SELECT url_hash, attempts, max_attempts FROM urls WHERE state='leased' AND lease_until < ?",
                    (now,),
                ).fetchall()
                for row in rows:
                    if row["attempts"] + 1 >= row["max_attempts"]:
                        conn.execute(
                            "UPDATE urls SET state='failed', attempts=attempts+1, error='lease expired', updated_at=? WHERE url_hash=?",
                            (now, row["url_hash"]),
                        )
                    else:
                        conn.execute(
                            """UPDATE urls SET state='pending', attempts=attempts+1, error='lease expired',
                               lease_until=NULL, worker=NULL, next_at=?, updated_at=? WHERE url_hash=?""",
                            (now, now, row["url_hash"]),
                        )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return len(rows)

    # ------------------------------------------------------------------- reads
    def stats(self) -> FrontierStats:
        conn = self._conn()
        counts = {r["state"]: r["n"] for r in conn.execute("SELECT state, COUNT(*) n FROM urls GROUP BY state")}
        domains = conn.execute("SELECT COUNT(DISTINCT domain) n FROM urls").fetchone()["n"]
        total = sum(counts.values())
        return FrontierStats(
            pending=counts.get("pending", 0),
            leased=counts.get("leased", 0),
            done=counts.get("done", 0),
            failed=counts.get("failed", 0),
            total=total,
            domains=domains,
        )

    def is_empty(self) -> bool:
        row = self._conn().execute("SELECT 1 FROM urls WHERE state IN ('pending','leased') LIMIT 1").fetchone()
        return row is None

    def get(self, url: str) -> Optional[UrlRecord]:
        row = self._conn().execute("SELECT * FROM urls WHERE url_hash=?", (url_key(url),)).fetchone()
        if row is None:
            return None
        return UrlRecord(row["url_hash"], row["url"], row["domain"], row["priority"], row["depth"], row["attempts"], row["state"])

    def iter_state(self, state: State, *, limit: Optional[int] = None) -> Iterator[UrlRecord]:
        sql = "SELECT * FROM urls WHERE state=? ORDER BY updated_at DESC"
        if limit:
            sql += f" LIMIT {int(limit)}"
        for row in self._conn().execute(sql, (state,)):
            yield UrlRecord(row["url_hash"], row["url"], row["domain"], row["priority"], row["depth"], row["attempts"], row["state"])

    def set_meta(self, key: str, value: str) -> None:
        self._conn().execute("INSERT INTO kv (k, v) VALUES (?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (key, value))

    def get_meta(self, key: str) -> Optional[str]:
        row = self._conn().execute("SELECT v FROM kv WHERE k=?", (key,)).fetchone()
        return row["v"] if row else None

    def gc_failed(self) -> int:
        cur = self._conn().execute("DELETE FROM urls WHERE state='failed'")
        return cur.rowcount
