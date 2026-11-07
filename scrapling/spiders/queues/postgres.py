"""Postgres-backed queue: several workers, one table, no double dispatch.

Delivery is **at least once**. A worker takes a row by marking it ``inflight`` and stamping a
lease; if that worker dies, the lease expires and another worker picks the row up again. The
same URL is therefore never handed to two workers at the same time, but it can be handed out
twice over the life of a crawl — callbacks must tolerate a repeat, which is the same contract
``MemoryQueue`` gives inside one process.

``asyncpg`` is optional; the rest of the library does not import it.
"""

import asyncio
import logging
import uuid
from typing import Any, Callable, Optional

try:
    import asyncpg
except ImportError as exc:  # pragma: no cover - depends on the environment
    raise ImportError(
        "PostgresQueue requires asyncpg. Install with: pip install scrapling[postgres]"
    ) from exc

import orjson

from scrapling.spiders.request import Request

log = logging.getLogger("scrapling.queue.postgres")

# ``orjson`` has no bytes type, and silently turning b"x" into "b'x'" would corrupt meta on the
# way back. Tag it instead, and refuse anything else it cannot represent rather than repr() it.
_BYTES_KEY = "$scrapling_bytes"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS scrapling_queue (
    fingerprint BYTEA PRIMARY KEY,
    url TEXT NOT NULL,
    request_json JSONB NOT NULL,
    priority INT NOT NULL DEFAULT 0,
    counter BIGSERIAL NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    lease_token TEXT,
    lease_expires_at TIMESTAMPTZ,
    enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_scrapling_queue_pending
    ON scrapling_queue (priority DESC, counter ASC)
    WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_scrapling_queue_lease
    ON scrapling_queue (lease_expires_at)
    WHERE status = 'inflight';
"""

_INSERT = (
    "INSERT INTO scrapling_queue (fingerprint, url, request_json, priority) "
    "VALUES ($1, $2, $3::jsonb, $4) ON CONFLICT (fingerprint) DO NOTHING "
    "RETURNING fingerprint"
)

# One statement: pick a candidate row without blocking on rows other workers hold, then claim it.
# ``now()`` and the lease length are both measured by the database, so worker clocks may disagree.
_CLAIM = """
WITH candidate AS (
    SELECT fingerprint FROM scrapling_queue
    WHERE status = 'pending'
       OR (status = 'inflight' AND lease_expires_at < now())
    ORDER BY priority DESC, counter ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
UPDATE scrapling_queue q
SET status = 'inflight',
    lease_token = $1,
    lease_expires_at = now() + make_interval(secs => $2)
FROM candidate
WHERE q.fingerprint = candidate.fingerprint
RETURNING q.request_json, q.fingerprint
"""

_COMPLETE = (
    "UPDATE scrapling_queue SET status = 'done', lease_token = NULL, lease_expires_at = NULL "
    "WHERE fingerprint = $1 AND lease_token = $2"
)

_IS_EMPTY = "SELECT NOT EXISTS (SELECT 1 FROM scrapling_queue WHERE status <> 'done')"

_SNAPSHOT_REQUESTS = (
    "SELECT request_json FROM scrapling_queue WHERE status <> 'done' "
    "ORDER BY priority DESC, counter ASC"
)

_SNAPSHOT_SEEN = "SELECT fingerprint FROM scrapling_queue"

_CLEAR_UNDONE = "DELETE FROM scrapling_queue WHERE status <> 'done'"


class QueueEmptyError(RuntimeError):
    """No row became available within ``idle_timeout``.

    ``CrawlerEngine`` only dequeues when it believes the queue is not empty, so this surfaces
    when another worker claimed the last row first. The crawl loop has no handler for it yet;
    treat it as "this worker is done" rather than as a fatal error.
    """


class PostgresQueue:
    """A :class:`~scrapling.spiders.queue.Queue` stored in Postgres.

    :param dsn: Anything ``asyncpg.connect`` accepts.
    :param lease_seconds: How long a claimed row stays reserved before another worker may take it.
    :param include_kwargs: Fingerprint options, same meaning as ``Spider.fp_include_kwargs``.
    :param include_headers: Fingerprint options, same meaning as ``Spider.fp_include_headers``.
    :param keep_fragments: Fingerprint options, same meaning as ``Spider.fp_keep_fragments``.
    :param callback_resolver: Maps a stored callback name back to a callable. Without it, a
        request loaded from the table keeps its name in ``_callback_name`` and the engine falls
        back to ``spider.parse`` — pass ``lambda name: getattr(spider, name, spider.parse)``
        from ``on_start`` to keep per-request callbacks across processes.
    :param idle_timeout: Seconds ``dequeue`` waits for a row before raising ``QueueEmptyError``.
    """

    def __init__(
        self,
        dsn: str,
        *,
        lease_seconds: float = 300.0,
        include_kwargs: bool = False,
        include_headers: bool = False,
        keep_fragments: bool = False,
        callback_resolver: Optional[Callable[[str], Any]] = None,
        idle_timeout: float = 5.0,
        poll_interval: float = 0.05,
        min_size: int = 0,
        max_size: int = 10,
    ) -> None:
        self._dsn = dsn
        self._lease_seconds = float(lease_seconds)
        self._fp_include_kwargs = include_kwargs
        self._fp_include_headers = include_headers
        self._fp_keep_fragments = keep_fragments
        self._resolve_callback = callback_resolver
        self._idle_timeout = float(idle_timeout)
        self._poll_interval = float(poll_interval)
        self._min_size = int(min_size)
        self._max_size = int(max_size)
        self._pool: Any = None
        # Cached answer to "does the table still hold unfinished rows?". ``is_empty`` is sync and
        # cannot query, so it reports this flag; ``start`` and ``complete`` refresh it. It can lag
        # behind another worker by one round trip, which is why this is a flag and not a count.
        self._has_undone = True

    # -- lifecycle -------------------------------------------------------

    async def start(self) -> None:
        """Create the pool and the table. Call it from ``spider.on_start``."""
        if self._pool is not None:
            return
        self._pool = await asyncpg.create_pool(
            dsn=self._dsn, min_size=self._min_size, max_size=self._max_size
        )
        async with self._pool.acquire() as connection:
            await connection.execute(_SCHEMA)
            self._has_undone = not await connection.fetchval(_IS_EMPTY)

    async def close(self) -> None:
        """Close the pool. Call it from ``spider.on_close``."""
        if self._pool is None:
            return
        pool, self._pool = self._pool, None
        await pool.close()

    @property
    def is_open(self) -> bool:
        return self._pool is not None

    def _require_pool(self) -> Any:
        if self._pool is None:
            raise RuntimeError("PostgresQueue.start() must be awaited before use")
        return self._pool

    # -- Queue protocol --------------------------------------------------

    async def enqueue(self, request: Request) -> bool:
        fingerprint = request.update_fingerprint(
            self._fp_include_kwargs, self._fp_include_headers, self._fp_keep_fragments
        )
        row = await self._execute_row(
            _INSERT,
            fingerprint,
            request.url,
            self._dump(request),
            request.priority,
        )
        if row is None:
            return False
        self._has_undone = True
        return True

    async def dequeue(self) -> Request:
        pool = self._require_pool()
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._idle_timeout
        while True:
            token = uuid.uuid4().hex
            async with pool.acquire() as connection:
                row = await connection.fetchrow(_CLAIM, token, self._lease_seconds)
            if row is not None:
                payload = row["request_json"]
                return self._load(payload, token)
            if loop.time() >= deadline:
                raise QueueEmptyError(
                    f"no request became available within {self._idle_timeout}s "
                    "(another worker may have claimed the last row)"
                )
            await asyncio.sleep(self._poll_interval)

    async def complete(self, request: Request) -> Optional[bool]:
        """Mark the row done. Returns None when the lease was already reclaimed."""
        token = getattr(request, "_lease_token", None)
        if not token:
            log.warning("complete() called for %s without a lease token; ignoring", request.url)
            return None
        fingerprint = request.update_fingerprint(
            self._fp_include_kwargs, self._fp_include_headers, self._fp_keep_fragments
        )
        status = await self._execute(_COMPLETE, fingerprint, token)
        if status.rsplit(" ", 1)[-1] != "1":
            log.warning(
                "lease for %s was reclaimed by another worker; the row stays inflight until its "
                "new owner finishes",
                request.url,
            )
            return None
        self._has_undone = not await self._execute_val(_IS_EMPTY)
        return True

    @property
    def is_empty(self) -> bool:
        """Last known state of the table. See the comment on ``_has_undone``."""
        return not self._has_undone

    async def is_empty_remote(self) -> bool:
        """Ask the database. ``is_empty`` cannot await, so this is the accurate variant."""
        return bool(await self._execute_val(_IS_EMPTY))

    async def snapshot(self) -> dict[str, Any]:
        requested = await self._execute_all(_SNAPSHOT_REQUESTS)
        seen = await self._execute_all(_SNAPSHOT_SEEN)
        return {
            "requests": [orjson.loads(row["request_json"]) for row in requested],
            "seen": [bytes(row["fingerprint"]).hex() for row in seen],
        }

    async def restore(self, data: dict[str, Any]) -> None:
        pool = self._require_pool()
        payloads = list(data.get("requests") or ())
        async with pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(_CLEAR_UNDONE)
                for payload in payloads:
                    request = self._load(payload, None)
                    fingerprint = request.update_fingerprint(
                        self._fp_include_kwargs, self._fp_include_headers, self._fp_keep_fragments
                    )
                    await connection.execute(
                        _INSERT, fingerprint, request.url, self._dump(request), request.priority
                    )
        self._has_undone = bool(payloads)

    # -- serialisation ---------------------------------------------------

    @staticmethod
    def _encode(value: Any) -> dict[str, str]:
        if isinstance(value, (bytes, bytearray)):
            return {_BYTES_KEY: bytes(value).hex()}
        raise TypeError(
            f"PostgresQueue cannot store {type(value).__name__} inside a request: meta and "
            "session kwargs must be JSON-serialisable (bytes are supported, other objects are not)"
        )

    @classmethod
    def _decode(cls, value: Any) -> Any:
        if isinstance(value, dict):
            if len(value) == 1 and _BYTES_KEY in value:
                return bytes.fromhex(value[_BYTES_KEY])
            return {key: cls._decode(item) for key, item in value.items()}
        if isinstance(value, list):
            return [cls._decode(item) for item in value]
        return value

    @staticmethod
    def _dump(request: Request) -> str:
        callback_name = getattr(request, "_callback_name", None)
        if not callback_name and request.callback is not None:
            callback_name = getattr(request.callback, "__name__", None)
        state = {
            "url": request.url,
            "sid": request.sid,
            "priority": request.priority,
            "dont_filter": request.dont_filter,
            "meta": request.meta,
            "_retry_count": request._retry_count,
            "_session_kwargs": request._session_kwargs,
            "callback_name": callback_name,
        }
        try:
            return orjson.dumps(state, default=PostgresQueue._encode).decode()
        except TypeError as exc:
            raise TypeError(
                f"PostgresQueue cannot store this request ({exc}): meta and session kwargs must "
                "be JSON-serialisable; bytes are supported, other objects are not"
            ) from exc

    def _load(self, payload: Any, lease_token: Optional[str]) -> Request:
        state = orjson.loads(payload) if isinstance(payload, (bytes, bytearray, str)) else dict(payload)
        request = Request(
            url=state["url"],
            sid=state.get("sid", ""),
            priority=int(state.get("priority", 0)),
            dont_filter=bool(state.get("dont_filter", False)),
            meta=self._decode(state.get("meta") or {}),
            _retry_count=int(state.get("_retry_count", 0)),
            **self._decode(state.get("_session_kwargs") or {}),
        )
        name = state.get("callback_name")
        if name:
            resolved = self._resolve_callback(name) if self._resolve_callback is not None else None
            if resolved is not None:
                request.callback = resolved
            else:
                request._callback_name = name
        if lease_token is not None:
            request._lease_token = lease_token
        return request

    # -- small wrappers so a fake pool only has to implement four methods --

    async def _execute(self, sql: str, *args: Any) -> str:
        pool = self._require_pool()
        async with pool.acquire() as connection:
            return await connection.execute(sql, *args)

    async def _execute_row(self, sql: str, *args: Any) -> Any:
        pool = self._require_pool()
        async with pool.acquire() as connection:
            return await connection.fetchrow(sql, *args)

    async def _execute_all(self, sql: str, *args: Any) -> list[Any]:
        pool = self._require_pool()
        async with pool.acquire() as connection:
            return list(await connection.fetch(sql, *args))

    async def _execute_val(self, sql: str, *args: Any) -> Any:
        pool = self._require_pool()
        async with pool.acquire() as connection:
            return await connection.fetchval(sql, *args)
