"""SQLite store: one row per URL, upsert on re-crawl, index on content_hash for dedup."""

import json
import sqlite3
from threading import RLock
from typing import Iterator, List, Optional, Tuple, Union

from scrapling.spiders.store import StoreClosed, StoredItem, url_hash

__all__ = ["SQLiteStore"]

_SCHEMA = """
CREATE TABLE IF NOT EXISTS items (
    url_hash     TEXT PRIMARY KEY,
    url          TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    item_json    TEXT NOT NULL,
    fetched_at   REAL NOT NULL,
    source_sid   TEXT NOT NULL DEFAULT '',
    meta_json    TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_content ON items(content_hash);
"""

_COLUMNS = "url_hash, url, content_hash, item_json, fetched_at, source_sid, meta_json"

_UPSERT = f"""
INSERT INTO items ({_COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(url_hash) DO UPDATE SET
    url          = excluded.url,
    content_hash = excluded.content_hash,
    item_json    = excluded.item_json,
    fetched_at   = excluded.fetched_at,
    source_sid   = excluded.source_sid,
    meta_json    = excluded.meta_json
"""


class SQLiteStore:
    """A file (or ``":memory:"``) with an ``items`` table.

    Pass an existing ``sqlite3.Connection`` to share the crawler's own database.
    """

    def __init__(self, target: Union[str, sqlite3.Connection] = ":memory:"):
        self._owns_connection = not isinstance(target, sqlite3.Connection)
        self._conn = sqlite3.connect(target, check_same_thread=False) if self._owns_connection else target
        self._lock = RLock()
        self._closed = False
        with self._lock:
            self._conn.executescript(_SCHEMA)
            self._conn.commit()

    def _check_open(self) -> None:
        if self._closed:
            raise StoreClosed("store is closed")

    @staticmethod
    def _row_to_item(row: Tuple) -> StoredItem:
        url_hash, url, content_hash, item_json, fetched_at, source_sid, meta_json = row
        return StoredItem(
            url=url,
            url_hash=url_hash,
            content_hash=content_hash,
            item=json.loads(item_json),
            fetched_at=fetched_at,
            source_sid=source_sid,
            meta=json.loads(meta_json or "{}"),
        )

    @classmethod
    def _values(cls, item: StoredItem) -> Tuple:
        return (
            item.url_hash,
            item.url,
            item.content_hash,
            json.dumps(item.item, ensure_ascii=False, default=str),
            item.fetched_at,
            item.source_sid,
            json.dumps(item.meta, ensure_ascii=False, default=str),
        )

    def put(self, item: StoredItem) -> bool:
        with self._lock:
            self._check_open()
            exists = self._conn.execute("SELECT 1 FROM items WHERE url_hash = ?", (item.url_hash,)).fetchone()
            self._conn.execute(_UPSERT, self._values(item))
            self._conn.commit()
            return exists is None

    def put_many(self, items: List[StoredItem]) -> Tuple[int, int]:
        """One transaction, one ``executemany``. Returns ``(new, updated)``."""
        with self._lock:
            self._check_open()
            known = {
                row[0]
                for row in self._conn.execute(
                    f"SELECT url_hash FROM items WHERE url_hash IN ({','.join('?' * len(items))})",
                    [item.url_hash for item in items],
                )
            } if items else set()
            if items:
                self._conn.executemany(_UPSERT, [self._values(item) for item in items])
                self._conn.commit()
            new = sum(1 for item in items if item.url_hash not in known)
            return new, len(items) - new

    def get(self, url: str) -> Optional[StoredItem]:
        with self._lock:
            self._check_open()
            row = self._conn.execute(
                f"SELECT {_COLUMNS} FROM items WHERE url_hash = ?", (url_hash(url),)
            ).fetchone()
        return None if row is None else self._row_to_item(row)

    def delete(self, url: str) -> bool:
        with self._lock:
            self._check_open()
            cursor = self._conn.execute("DELETE FROM items WHERE url_hash = ?", (url_hash(url),))
            self._conn.commit()
            return cursor.rowcount > 0

    def count(self) -> int:
        with self._lock:
            self._check_open()
            return int(self._conn.execute("SELECT COUNT(*) FROM items").fetchone()[0])

    def iter_items(self, *, limit: int = 100, offset: int = 0) -> Iterator[StoredItem]:
        with self._lock:
            self._check_open()
            rows = self._conn.execute(
                f"SELECT {_COLUMNS} FROM items ORDER BY url_hash LIMIT ? OFFSET ?", (limit, offset)
            ).fetchall()
        for row in rows:
            yield self._row_to_item(row)

    def has_content(self, content_hash: str) -> bool:
        with self._lock:
            self._check_open()
            return self._conn.execute("SELECT 1 FROM items WHERE content_hash = ? LIMIT 1", (content_hash,)).fetchone() is not None

    def find_by_content(self, content_hash: str) -> List[StoredItem]:
        with self._lock:
            self._check_open()
            rows = self._conn.execute(
                f"SELECT {_COLUMNS} FROM items WHERE content_hash = ? ORDER BY url_hash", (content_hash,)
            ).fetchall()
        return [self._row_to_item(row) for row in rows]

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            if self._owns_connection:
                self._conn.close()
