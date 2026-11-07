"""Content dedup on top of a store.

The store answers "have I seen this exact content before" in O(1) via an index; this layer
adds a bounded in-memory window so a burst of duplicates in one crawl never reaches SQL.
"""

from collections import OrderedDict
from threading import RLock
from typing import Any, Optional

from scrapling.spiders.store import StoredItem

__all__ = ["ContentDeduplicator"]


class ContentDeduplicator:
    """Remembers recent ``content_hash`` values, and asks the store about older ones.

    ``window`` caps the memory: oldest hashes fall out first, and the store index still
    catches them afterwards, so eviction costs a query, never a false negative.
    """

    def __init__(self, store: Any, *, window: int = 10000):
        self._store = store
        self._window = max(1, int(window))
        self._seen: OrderedDict[str, None] = OrderedDict()
        self._lock = RLock()

    @property
    def window(self) -> int:
        return self._window

    def __len__(self) -> int:
        return len(self._seen)

    def is_duplicate(self, item: StoredItem) -> bool:
        """True when this content hash is in the window, or already in the store."""
        digest = item.content_hash
        with self._lock:
            if digest in self._seen:
                self._seen.move_to_end(digest)
                return True

        checker = getattr(self._store, "has_content", None)
        return bool(checker(digest)) if checker is not None else False

    def mark_seen(self, item: StoredItem) -> None:
        """Remember a hash. Called after a successful ``store.put``."""
        digest = item.content_hash
        with self._lock:
            self._seen[digest] = None
            self._seen.move_to_end(digest)
            while len(self._seen) > self._window:
                self._seen.popitem(last=False)

    def forget(self, item: StoredItem) -> None:
        """Drop a hash from the window so the store decides next time."""
        with self._lock:
            self._seen.pop(item.content_hash, None)

    def clear(self) -> None:
        with self._lock:
            self._seen.clear()

    def accept(self, item: StoredItem, *, store: Optional[Any] = None) -> bool:
        """``is_duplicate`` + ``mark_seen`` in one call. True means "new, worth storing"."""
        if self.is_duplicate(item):
            return False
        target = store if store is not None else self._store
        placer = getattr(target, "put", None)
        if placer is not None:
            placer(item)
        self.mark_seen(item)
        return True
