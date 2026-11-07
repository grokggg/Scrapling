"""Storage backends for :class:`scrapling.spiders.incremental.IncrementalCache`.

Both backends answer the same three calls, so the cache above never learns where the
bytes live:

    get(url) -> (CacheEntry, bytes) | None
    put(entry, body) -> None
    delete(url) -> None
"""

import json
import re
import sqlite3
from dataclasses import asdict
from threading import Lock
from typing import Dict, Optional, Protocol, Tuple, Union, runtime_checkable

from scrapling.spiders.incremental import CacheEntry

__all__ = ["IncrementalStorage", "MemoryStorage", "SQLiteStorage"]

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@runtime_checkable
class IncrementalStorage(Protocol):
    """Minimum surface the cache needs. Implement these three and the cache works."""

    def get(self, url: str) -> Optional[Tuple[CacheEntry, bytes]]: ...

    def put(self, entry: CacheEntry, body: bytes) -> None: ...

    def delete(self, url: str) -> None: ...


class MemoryStorage:
    """Per-process dictionary. Fast, gone when the process is."""

    def __init__(self) -> None:
        self._entries: Dict[str, Tuple[CacheEntry, bytes]] = {}
        self._lock = Lock()

    def get(self, url: str) -> Optional[Tuple[CacheEntry, bytes]]:
        with self._lock:
            return self._entries.get(url)

    def put(self, entry: CacheEntry, body: bytes) -> None:
        with self._lock:
            self._entries[entry.url] = (entry, bytes(body))

    def delete(self, url: str) -> None:
        with self._lock:
            self._entries.pop(url, None)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()

    def __len__(self) -> int:
        return len(self._entries)

    def __contains__(self, url: str) -> bool:
        return url in self._entries


class SQLiteStorage:
    """One row per URL: the entry as JSON, the body as a BLOB.

    Pass a path for a file-backed cache, ``":memory:"`` for a throwaway one, or an existing
    ``sqlite3.Connection`` (e.g. the crawler's own DB) to share it.
    """

    def __init__(self, target: Union[str, sqlite3.Connection] = ":memory:", *, table: str = "incremental_cache"):
        if not _IDENT.match(table):
            raise ValueError(f"invalid table name: {table!r}")
        self._table = table
        self._owns_connection = not isinstance(target, sqlite3.Connection)
        self._conn = sqlite3.connect(target, check_same_thread=False) if self._owns_connection else target
        self._lock = Lock()
        with self._lock:
            self._conn.execute(
                f"CREATE TABLE IF NOT EXISTS {self._table} ("
                "url TEXT PRIMARY KEY, entry_json TEXT NOT NULL, body_bytes BLOB NOT NULL)"
            )
            self._conn.commit()

    def get(self, url: str) -> Optional[Tuple[CacheEntry, bytes]]:
        with self._lock:
            row = self._conn.execute(
                f"SELECT entry_json, body_bytes FROM {self._table} WHERE url = ?", (url,)
            ).fetchone()
        if row is None:
            return None
        entry_json, body = row
        return CacheEntry(**json.loads(entry_json)), bytes(body)

    def put(self, entry: CacheEntry, body: bytes) -> None:
        with self._lock:
            self._conn.execute(
                f"INSERT OR REPLACE INTO {self._table} (url, entry_json, body_bytes) VALUES (?, ?, ?)",
                (entry.url, json.dumps(asdict(entry)), bytes(body)),
            )
            self._conn.commit()

    def delete(self, url: str) -> None:
        with self._lock:
            self._conn.execute(f"DELETE FROM {self._table} WHERE url = ?", (url,))
            self._conn.commit()

    def clear(self) -> None:
        with self._lock:
            self._conn.execute(f"DELETE FROM {self._table}")
            self._conn.commit()

    def close(self) -> None:
        """Only closes a connection this object opened itself."""
        if self._owns_connection:
            with self._lock:
                self._conn.close()

    def __len__(self) -> int:
        with self._lock:
            return int(self._conn.execute(f"SELECT COUNT(*) FROM {self._table}").fetchone()[0])

    def __contains__(self, url: str) -> bool:
        return self.get(url) is not None
