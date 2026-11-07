"""Incremental crawling: remember what a URL returned, skip re-downloading the unchanged parts.

Pure logic, no IO of its own. Every byte lives in an ``IncrementalStorage`` backend
(see ``incremental_storage``), so the same cache works in memory or on disk.
"""

from dataclasses import asdict, dataclass, field
from hashlib import sha256
from time import time
from typing import Any, Dict, Optional, Tuple

from scrapling.engines.toolbelt.custom import Response

__all__ = ["CacheEntry", "IncrementalCache"]


@dataclass
class CacheEntry:
    """What one URL returned last time, enough to make a conditional request later."""

    url: str
    etag: Optional[str] = None
    last_modified: Optional[str] = None
    body_sha256: str = ""
    body_size: int = 0
    headers: Dict[str, Any] = field(default_factory=dict)
    status: int = 200
    stored_at: float = 0.0
    ttl_seconds: float = 86400.0  # 24h 后过期

    def is_expired(self, *, now: Optional[float] = None) -> bool:
        """A zero or negative TTL means "never expires" — useful for immutable assets."""
        if self.ttl_seconds <= 0:
            return False
        return (time() if now is None else now) - self.stored_at > self.ttl_seconds


def _header(headers: Any, name: str) -> Optional[str]:
    """Case-insensitive header lookup that survives list values."""
    target = name.lower()
    items = headers.items() if hasattr(headers, "items") else (headers or [])
    for key, value in items:
        if str(key).lower() != target:
            continue
        if isinstance(value, (list, tuple)):
            return str(value[0]) if value else None
        return str(value)
    return None


class IncrementalCache:
    """ETag / Last-Modified bookkeeping on top of any storage backend."""

    def __init__(self, storage: Any, ttl_seconds: float = 86400.0):
        """``storage`` is anything honouring the ``IncrementalStorage`` protocol."""
        self._storage = storage
        self._ttl = ttl_seconds

    @property
    def storage(self) -> Any:
        return self._storage

    def get(self, url: str) -> Optional[CacheEntry]:
        """The cached entry for ``url``, or ``None`` when missing or expired."""
        stored = self._storage.get(url)
        if stored is None:
            return None
        entry = stored[0]
        return None if entry.is_expired() else entry

    def put(self, url: str, response: Response, *, body_bytes: Optional[bytes] = None) -> CacheEntry:
        """Record what ``response`` returned. ``body_bytes`` overrides ``response.body``."""
        body = response.body if body_bytes is None else body_bytes
        body = body or b""
        entry = CacheEntry(
            url=url,
            etag=_header(response.headers, "etag"),
            last_modified=_header(response.headers, "last-modified"),
            body_sha256=sha256(body).hexdigest(),
            body_size=len(body),
            headers=dict(response.headers or {}),
            status=response.status,
            stored_at=time(),
            ttl_seconds=self._ttl,
        )
        self._storage.put(entry, body)
        return entry

    def delete(self, url: str) -> None:
        self._storage.delete(url)

    def conditional_headers(self, url: str) -> Dict[str, str]:
        """Headers to send so the server can answer 304. Empty when nothing is cached."""
        entry = self.get(url)
        if entry is None:
            return {}
        headers: Dict[str, str] = {}
        if entry.etag:
            headers["If-None-Match"] = entry.etag
        if entry.last_modified:
            headers["If-Modified-Since"] = entry.last_modified
        return headers

    def is_not_modified(self, response: Response) -> bool:
        """304, or a 200 whose body hashes the same as last time.

        Plenty of servers and CDNs ignore conditional headers and answer 200 with the
        identical body; hash comparison is what actually saves the parse in that case.
        """
        if getattr(response, "status", None) == 304:
            return True
        if getattr(response, "status", None) != 200:
            return False
        entry = self.get(response.url)
        if entry is None:
            return False
        return sha256(response.body or b"").hexdigest() == entry.body_sha256

    def build_response_from_cache(self, url: str) -> Optional[Response]:
        """Rebuild a ``Response`` from the stored body, for when the server said 304."""
        stored = self._storage.get(url)
        if stored is None:
            return None
        entry, body = stored
        return Response(
            url=entry.url,
            content=body,
            status=entry.status,
            reason="OK",
            cookies={},
            headers=dict(entry.headers),
            request_headers={},
            _log=False,
        )

    def to_dict(self, entry: CacheEntry) -> Dict[str, Any]:
        """Flat view of an entry, handy for stats and logs."""
        return asdict(entry)

    def stats(self) -> Dict[str, Any]:
        return {"ttl_seconds": self._ttl, "backend": type(self._storage).__name__}

    def __contains__(self, url: str) -> bool:
        return self.get(url) is not None

    def __len__(self) -> int:
        return len(self._storage) if hasattr(self._storage, "__len__") else 0
