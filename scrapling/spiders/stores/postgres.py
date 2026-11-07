"""Postgres store: shared across machines, JSONB payloads, async via asyncpg.

Optional dependency — ``pip install scrapling[pg]``. Everything else in the framework works
without it; importing this module never imports asyncpg, only opening a store does.
"""

import json
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

from scrapling.spiders.store import StoreClosed, StoredItem, url_hash

__all__ = ["PostgresStore"]

_SCHEMA = """
CREATE TABLE IF NOT EXISTS items (
    url_hash     TEXT PRIMARY KEY,
    url          TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    item         JSONB NOT NULL,
    fetched_at   DOUBLE PRECISION NOT NULL,
    source_sid   TEXT NOT NULL DEFAULT '',
    meta         JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_content ON items(content_hash);
"""

_COLUMNS = "url_hash, url, content_hash, item, fetched_at, source_sid, meta"

_UPSERT = f"""
INSERT INTO items ({_COLUMNS}) VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7::jsonb)
ON CONFLICT (url_hash) DO UPDATE SET
    url          = excluded.url,
    content_hash = excluded.content_hash,
    item         = excluded.item,
    fetched_at   = excluded.fetched_at,
    source_sid   = excluded.source_sid,
    meta         = excluded.meta
"""


def _loads(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    return json.loads(value) if isinstance(value, (str, bytes, bytearray)) else dict(value)


class PostgresStore:
    """Async store sharing one connection pool across the crawl.

    ``await store.open()`` before use, ``await store.close()`` when done. The pool is
    created lazily so the object can be built in a sync context (e.g. a spider class).
    """

    def __init__(
        self,
        dsn: str = "",
        *,
        min_size: int = 1,
        max_size: int = 10,
        table: str = "items",
        pool: Any = None,
    ):
        self._dsn = dsn
        self._min_size = min_size
        self._max_size = max_size
        self._table = table
        self._pool = pool
        self._owns_pool = pool is None
        self._closed = False

    async def open(self) -> "PostgresStore":
        if self._pool is None:
            try:
                import asyncpg  # noqa: PLC0415  (optional dependency)
            except ImportError as exc:  # pragma: no cover - depends on the install
                raise ImportError(
                    "PostgresStore needs asyncpg: pip install scrapling[pg]"
                ) from exc
            self._pool = await asyncpg.create_pool(
                self._dsn, min_size=self._min_size, max_size=self._max_size
            )
        async with self._pool.acquire() as conn:
            await conn.execute(_SCHEMA.replace("items", self._table))
        return self

    def _check_open(self) -> None:
        if self._closed or self._pool is None:
            raise StoreClosed("store is closed or never opened")

    async def put(self, item: StoredItem) -> bool:
        self._check_open()
        async with self._pool.acquire() as conn:
            exists = await conn.fetchval(
                f"SELECT 1 FROM {self._table} WHERE url_hash = $1", item.url_hash
            )
            await conn.execute(
                _UPSERT.replace("items", self._table),
                item.url_hash,
                item.url,
                item.content_hash,
                json.dumps(item.item, ensure_ascii=False, default=str),
                item.fetched_at,
                item.source_sid,
                json.dumps(item.meta, ensure_ascii=False, default=str),
            )
        return exists is None

    async def put_many(self, items: List[StoredItem]) -> Tuple[int, int]:
        self._check_open()
        if not items:
            return 0, 0
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                known = {
                    row["url_hash"]
                    for row in await conn.fetch(
                        f"SELECT url_hash FROM {self._table} WHERE url_hash = ANY($1::text[])",
                        [item.url_hash for item in items],
                    )
                }
                await conn.executemany(
                    _UPSERT.replace("items", self._table),
                    [
                        (
                            item.url_hash,
                            item.url,
                            item.content_hash,
                            json.dumps(item.item, ensure_ascii=False, default=str),
                            item.fetched_at,
                            item.source_sid,
                            json.dumps(item.meta, ensure_ascii=False, default=str),
                        )
                        for item in items
                    ],
                )
        new = sum(1 for item in items if item.url_hash not in known)
        return new, len(items) - new

    async def get(self, url: str) -> Optional[StoredItem]:
        self._check_open()
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT {_COLUMNS} FROM {self._table} WHERE url_hash = $1", url_hash(url)
            )
        return None if row is None else self._to_item(row)

    async def delete(self, url: str) -> bool:
        self._check_open()
        async with self._pool.acquire() as conn:
            status = await conn.execute(
                f"DELETE FROM {self._table} WHERE url_hash = $1", url_hash(url)
            )
        return status.rsplit(" ", 1)[-1] != "0"

    async def count(self) -> int:
        self._check_open()
        async with self._pool.acquire() as conn:
            return int(await conn.fetchval(f"SELECT COUNT(*) FROM {self._table}"))

    async def iter_items(self, *, limit: int = 100, offset: int = 0) -> AsyncIterator[StoredItem]:
        self._check_open()
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT {_COLUMNS} FROM {self._table} ORDER BY url_hash LIMIT $1 OFFSET $2",
                limit,
                offset,
            )
        for row in rows:
            yield self._to_item(row)

    async def has_content(self, content_hash: str) -> bool:
        self._check_open()
        async with self._pool.acquire() as conn:
            return (
                await conn.fetchval(
                    f"SELECT 1 FROM {self._table} WHERE content_hash = $1 LIMIT 1", content_hash
                )
            ) is not None

    async def find_by_content(self, content_hash: str) -> List[StoredItem]:
        self._check_open()
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT {_COLUMNS} FROM {self._table} WHERE content_hash = $1 ORDER BY url_hash",
                content_hash,
            )
        return [self._to_item(row) for row in rows]

    @staticmethod
    def _to_item(row: Any) -> StoredItem:
        return StoredItem(
            url=row["url"],
            url_hash=row["url_hash"],
            content_hash=row["content_hash"],
            item=_loads(row["item"]),
            fetched_at=row["fetched_at"],
            source_sid=row["source_sid"],
            meta=_loads(row["meta"]),
        )

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._owns_pool and self._pool is not None:
            await self._pool.close()
