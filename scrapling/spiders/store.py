"""Pluggable result storage.

The crawler used to keep results in an in-memory ``ItemList`` and dump them at the end.
This is the replacement: one record per URL, upsert on re-crawl, and two hashes so a URL
can be found again (``url_hash``) and identical content can be recognised (``content_hash``).

Backends live in :mod:`scrapling.spiders.stores`; pick one, hand it to ``Spider.store``.
"""

import json
from dataclasses import dataclass, field
from hashlib import blake2b
from time import time
from typing import Any, AsyncIterator, Dict, Iterator, Optional, Protocol, runtime_checkable

__all__ = [
    "AsyncStore",
    "Store",
    "StoreClosed",
    "StoredItem",
    "content_hash",
    "url_hash",
]


class StoreClosed(RuntimeError):
    """Raised when a backend is used after ``close()``."""


def _digest(payload: bytes) -> str:
    # blake2b, not sha256: this runs once per item and only ever compares within one install.
    return blake2b(payload, digest_size=32).hexdigest()


def canonical_json(value: Any) -> str:
    """Key order, spacing and non-ASCII spelled the same way every time."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def url_hash(url: str) -> str:
    return _digest(url.encode("utf-8"))


def _computed_content_hash(item: Any) -> str:
    return _digest(canonical_json(item).encode("utf-8"))


#: Public name. `StoredItem.content_hash` shadows it inside the dataclass, hence the alias.
content_hash = _computed_content_hash


@dataclass
class StoredItem:
    """One scraped row. Hashes and timestamp fill themselves in if you leave them blank."""

    url: str
    url_hash: str = ""
    content_hash: str = ""
    item: Dict[str, Any] = field(default_factory=dict)
    fetched_at: float = 0.0
    source_sid: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.url_hash:
            self.url_hash = url_hash(self.url)
        if not self.content_hash:
            self.content_hash = _computed_content_hash(self.item)
        if not self.fetched_at:
            self.fetched_at = time()


@runtime_checkable
class Store(Protocol):
    """Synchronous backend. ``put`` returns True when the row is new, False when updated."""

    def put(self, item: StoredItem) -> bool: ...

    def get(self, url: str) -> Optional[StoredItem]: ...

    def delete(self, url: str) -> bool: ...

    def count(self) -> int: ...

    def iter_items(self, *, limit: int = 100, offset: int = 0) -> Iterator[StoredItem]: ...

    def close(self) -> None: ...


@runtime_checkable
class AsyncStore(Protocol):
    """Async backend, same contract with awaitables (Postgres)."""

    async def put(self, item: StoredItem) -> bool: ...

    async def get(self, url: str) -> Optional[StoredItem]: ...

    async def delete(self, url: str) -> bool: ...

    async def count(self) -> int: ...

    async def iter_items(self, *, limit: int = 100, offset: int = 0) -> AsyncIterator[StoredItem]: ...

    async def close(self) -> None: ...
