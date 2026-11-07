"""In-process store: a dict keyed by ``url_hash``, guarded by a re-entrant lock."""

from threading import RLock
from typing import Dict, Iterator, List, Optional

from scrapling.spiders.store import StoreClosed, StoredItem, url_hash

__all__ = ["MemoryStore"]


class MemoryStore:
    """Zero setup, no persistence. Useful for tests and single-run jobs."""

    def __init__(self) -> None:
        self._rows: Dict[str, StoredItem] = {}
        self._lock = RLock()
        self._closed = False

    def _check_open(self) -> None:
        if self._closed:
            raise StoreClosed("store is closed")

    def put(self, item: StoredItem) -> bool:
        with self._lock:
            self._check_open()
            is_new = item.url_hash not in self._rows
            self._rows[item.url_hash] = item
            return is_new

    def put_many(self, items: List[StoredItem]) -> tuple[int, int]:
        """Returns ``(new, updated)``."""
        with self._lock:
            self._check_open()
            new = sum(1 for item in items if item.url_hash not in self._rows)
            for item in items:
                self._rows[item.url_hash] = item
            return new, len(items) - new

    def get(self, url: str) -> Optional[StoredItem]:
        with self._lock:
            self._check_open()
            return self._rows.get(url_hash(url))

    def delete(self, url: str) -> bool:
        with self._lock:
            self._check_open()
            return self._rows.pop(url_hash(url), None) is not None

    def count(self) -> int:
        with self._lock:
            self._check_open()
            return len(self._rows)

    def iter_items(self, *, limit: int = 100, offset: int = 0) -> Iterator[StoredItem]:
        with self._lock:
            self._check_open()
            rows = sorted(self._rows.values(), key=lambda row: row.url_hash)
        yield from rows[offset : offset + limit]

    def has_content(self, content_hash: str) -> bool:
        """Any row with this content hash? Backs the dedup layer."""
        with self._lock:
            self._check_open()
            return any(row.content_hash == content_hash for row in self._rows.values())

    def find_by_content(self, content_hash: str) -> List[StoredItem]:
        with self._lock:
            self._check_open()
            return [row for row in self._rows.values() if row.content_hash == content_hash]

    def close(self) -> None:
        with self._lock:
            self._closed = True
            self._rows.clear()
