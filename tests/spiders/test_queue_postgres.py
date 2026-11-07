"""PostgresQueue against an in-memory fake pool: no server required.

The SQL is not re-implemented faithfully — the fake only answers the statements this module
actually issues, matched by their leading keywords. What the tests pin down is our side of the
contract: dedup, claim, lease expiry, lease-mismatch on complete, snapshot/restore.
"""

import pytest

pytest.importorskip("asyncpg")

from scrapling.spiders.queues.postgres import PostgresQueue, QueueEmptyError  # noqa: E402
from scrapling.spiders.request import Request  # noqa: E402

URL_A = "https://example.com/a"
URL_B = "https://example.com/b"


class _Acquired:
    def __init__(self, pool):
        self._pool = pool

    async def __aenter__(self):
        return _Conn(self._pool)

    async def __aexit__(self, *exc_info):
        return False


class _Transaction:
    def __init__(self, pool):
        self._pool = pool
        self._backup = None

    async def __aenter__(self):
        self._backup = {k: dict(v) for k, v in self._pool.rows.items()}
        return self

    async def __aexit__(self, exc_type, *exc_info):
        if exc_type is not None:
            self._pool.rows = self._backup
        return False


class FakePool:
    """Just enough of asyncpg.Pool for PostgresQueue."""

    def __init__(self):
        self.rows = {}
        self.counter = 0
        self.now = 1000.0
        self.closed = False
        self.statements = []

    def acquire(self):
        return _Acquired(self)

    async def close(self):
        self.closed = True

    # helpers used by the assertions
    def status(self, url: str) -> str:
        for row in self.rows.values():
            if row["url"] == url:
                return row["status"]
        raise KeyError(url)


class _Conn:
    def __init__(self, pool: FakePool):
        self.pool = pool

    def transaction(self):
        return _Transaction(self.pool)

    async def execute(self, sql, *args):
        assert "now()" not in sql or "make_interval" in sql, sql
        s = " ".join(sql.split())
        p = self.pool
        p.statements.append(s.split(" ")[0])
        if s.startswith("CREATE TABLE") or s.startswith("CREATE INDEX"):
            return "OK"
        if s.startswith("INSERT INTO scrapling_queue"):
            fingerprint, url, payload, priority = args
            if fingerprint in p.rows:
                return "INSERT 0 0"
            p.counter += 1
            p.rows[fingerprint] = {
                "url": url,
                "request_json": payload,
                "priority": priority,
                "counter": p.counter,
                "status": "pending",
                "lease_token": None,
                "lease_expires_at": None,
            }
            return "INSERT 0 1"
        if s.startswith("UPDATE scrapling_queue SET status = 'done'"):
            fingerprint, token = args
            row = p.rows.get(fingerprint)
            if row is None or row["lease_token"] != token:
                return "UPDATE 0"
            row["status"] = "done"
            row["lease_token"] = None
            row["lease_expires_at"] = None
            return "UPDATE 1"
        if s.startswith("DELETE FROM scrapling_queue WHERE status <> 'done'"):
            doomed = [k for k, v in p.rows.items() if v["status"] != "done"]
            for key in doomed:
                del p.rows[key]
            return f"DELETE {len(doomed)}"
        raise AssertionError(f"unexpected execute: {s[:60]}")

    async def fetchrow(self, sql, *args):
        s = " ".join(sql.split())
        p = self.pool
        if s.startswith("WITH candidate"):
            token, lease_seconds = args
            candidates = [
                (key, row)
                for key, row in p.rows.items()
                if row["status"] == "pending"
                or (
                    row["status"] == "inflight"
                    and row["lease_expires_at"] is not None
                    and row["lease_expires_at"] < p.now
                )
            ]
            if not candidates:
                return None
            key, row = min(candidates, key=lambda item: (-item[1]["priority"], item[1]["counter"]))
            row["status"] = "inflight"
            row["lease_token"] = token
            row["lease_expires_at"] = p.now + lease_seconds
            return {"request_json": row["request_json"], "fingerprint": key}
        if s.startswith("INSERT INTO scrapling_queue"):
            result = await self.execute(sql, *args)
            if result.endswith(" 0"):
                return None
            return {"fingerprint": args[0]}
        raise AssertionError(f"unexpected fetchrow: {s[:60]}")

    async def fetch(self, sql, *args):
        s = " ".join(sql.split())
        p = self.pool
        if s.startswith("SELECT request_json FROM scrapling_queue WHERE status <> 'done'"):
            rows = sorted(
                (r for r in p.rows.values() if r["status"] != "done"),
                key=lambda r: (-r["priority"], r["counter"]),
            )
            return [{"request_json": r["request_json"]} for r in rows]
        if s.startswith("SELECT fingerprint FROM scrapling_queue"):
            return [{"fingerprint": key} for key in p.rows]
        raise AssertionError(f"unexpected fetch: {s[:60]}")

    async def fetchval(self, sql, *args):
        s = " ".join(sql.split())
        if not s.startswith("SELECT NOT EXISTS"):
            raise AssertionError(f"unexpected fetchval: {s[:60]}")
        return not any(row["status"] != "done" for row in self.pool.rows.values())


def _queue(**kwargs) -> tuple[PostgresQueue, FakePool]:
    queue = PostgresQueue("postgresql://fake/db", **kwargs)
    pool = FakePool()
    queue._pool = pool
    return queue, pool


def _request(url: str = URL_A, **kwargs) -> Request:
    return Request(url, **kwargs)


@pytest.mark.asyncio
async def test_enqueue_returns_true_for_a_new_fingerprint():
    queue, pool = _queue()

    assert await queue.enqueue(_request()) is True
    assert len(pool.rows) == 1


@pytest.mark.asyncio
async def test_enqueue_returns_false_when_the_fingerprint_is_known():
    queue, pool = _queue()
    await queue.enqueue(_request())

    assert await queue.enqueue(_request()) is False
    assert len(pool.rows) == 1


@pytest.mark.asyncio
async def test_dequeue_claims_the_row_and_attaches_the_lease():
    queue, pool = _queue()
    await queue.enqueue(_request(sid="s1", priority=2, meta={"page": 7}, headers={"a": "b"}))

    request = await queue.dequeue()

    assert request.url == URL_A
    assert request.sid == "s1"
    assert request.priority == 2
    assert request.meta == {"page": 7}
    assert request._session_kwargs == {"headers": {"a": "b"}}
    assert request._lease_token
    assert pool.status(URL_A) == "inflight"


@pytest.mark.asyncio
async def test_dequeue_honours_priority():
    queue, pool = _queue()
    await queue.enqueue(_request(URL_A, priority=1))
    await queue.enqueue(_request(URL_B, priority=9))

    assert (await queue.dequeue()).url == URL_B


@pytest.mark.asyncio
async def test_an_expired_lease_is_handed_to_the_next_worker():
    queue, pool = _queue(lease_seconds=30)
    await queue.enqueue(_request())

    first = await queue.dequeue()
    assert first.url == URL_A
    assert pool.status(URL_A) == "inflight"

    pool.now += 31  # the first worker "died"; its lease lapsed
    second = await queue.dequeue()

    assert second.url == URL_A
    assert second._lease_token != first._lease_token


@pytest.mark.asyncio
async def test_complete_with_the_current_lease_marks_the_row_done():
    queue, pool = _queue()
    await queue.enqueue(_request())
    request = await queue.dequeue()

    assert await queue.complete(request) is True
    assert pool.status(URL_A) == "done"


@pytest.mark.asyncio
async def test_complete_with_a_stale_lease_changes_nothing():
    queue, pool = _queue(lease_seconds=30)
    await queue.enqueue(_request())
    first = await queue.dequeue()
    pool.now += 31
    await queue.dequeue()  # someone else reclaimed it
    token_after_reclaim = pool.rows[next(iter(pool.rows))]["lease_token"]

    assert await queue.complete(first) is None
    assert pool.status(URL_A) == "inflight"
    assert pool.rows[next(iter(pool.rows))]["lease_token"] == token_after_reclaim


@pytest.mark.asyncio
async def test_complete_without_a_token_is_ignored():
    queue, pool = _queue()
    await queue.enqueue(_request())
    await queue.dequeue()

    assert await queue.complete(_request()) is None
    assert pool.status(URL_A) == "inflight"


@pytest.mark.asyncio
async def test_is_empty_reflects_undone_and_done_rows():
    queue, pool = _queue()
    await queue.enqueue(_request())

    assert await queue.is_empty_remote() is False
    assert queue.is_empty is False

    request = await queue.dequeue()
    await queue.complete(request)

    assert await queue.is_empty_remote() is True
    assert queue.is_empty is True


@pytest.mark.asyncio
async def test_snapshot_returns_every_undone_row_plus_the_seen_set():
    queue, pool = _queue()
    await queue.enqueue(_request(URL_A, priority=1))
    await queue.enqueue(_request(URL_B, priority=5))
    await queue.dequeue()  # B goes inflight; still unfinished, still in the snapshot

    snapshot = await queue.snapshot()

    assert [item["url"] for item in snapshot["requests"]] == [URL_B, URL_A]
    assert len(snapshot["seen"]) == 2


@pytest.mark.asyncio
async def test_restore_replaces_the_undone_rows():
    queue, pool = _queue()
    await queue.enqueue(_request(URL_A))
    await queue.enqueue(_request(URL_B))
    done = await queue.dequeue()
    await queue.complete(done)
    snapshot = await queue.snapshot()
    await queue.enqueue(_request("https://example.com/late"))

    await queue.restore({"requests": snapshot["requests"], "seen": snapshot["seen"]})

    urls = sorted(row["url"] for row in pool.rows.values())
    assert urls == sorted([URL_A, URL_B]), "the request enqueued after the snapshot is dropped"
    assert pool.status(URL_A) == "done", "finished rows stay: they are the dedup history"
    assert pool.status(URL_B) == "pending", "unfinished rows come back as fresh work"


@pytest.mark.asyncio
async def test_dequeue_raises_when_the_table_stays_empty():
    queue, _ = _queue(idle_timeout=0)

    with pytest.raises(QueueEmptyError, match="no request became available"):
        await queue.dequeue()


@pytest.mark.asyncio
async def test_callback_name_survives_the_round_trip():
    def detail(response):  # pragma: no cover - only its name matters
        return response

    queue, _ = _queue()
    request = _request()
    request.callback = detail
    await queue.enqueue(request)

    loaded = await queue.dequeue()
    assert getattr(loaded, "_callback_name", None) == "detail"
    assert loaded.callback is None


@pytest.mark.asyncio
async def test_callback_resolver_rebinds_the_callable():
    def detail(response):  # pragma: no cover - identity only
        return response

    queue, _ = _queue(callback_resolver=lambda name: detail if name == "detail" else None)
    request = _request()
    request.callback = detail
    await queue.enqueue(request)

    loaded = await queue.dequeue()
    assert loaded.callback is detail
    assert not hasattr(loaded, "_callback_name")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "meta",
    [
        {"k": [1, 2], "b": b"raw"},
        {"nested": {"deep": [b"\x00\xff", {"again": b""}]}},
        {"sid": "918", "flag": None, "n": 1.5, "t": True},
    ],
)
async def test_meta_survives_the_round_trip_including_bytes(meta):
    queue, _ = _queue()
    await queue.enqueue(_request(meta=meta))

    assert (await queue.dequeue()).meta == meta


@pytest.mark.asyncio
async def test_uncopyable_meta_fails_loudly_instead_of_being_stringified():
    queue, _ = _queue()
    request = _request(meta={"bad": object()})

    with pytest.raises(TypeError, match="JSON-serialisable"):
        await queue.enqueue(request)


@pytest.mark.asyncio
async def test_use_before_start_is_an_explicit_error():
    queue = PostgresQueue("postgresql://fake/db")

    with pytest.raises(RuntimeError, match="start"):
        await queue.enqueue(_request())


# -- real database, only when a DSN is provided ------------------------------------------------


PG_DSN = __import__("os").environ.get("SCRAPLING_TEST_PG_DSN")


@pytest.mark.skipif(not PG_DSN, reason="SCRAPLING_TEST_PG_DSN is not set")
@pytest.mark.asyncio
async def test_full_cycle_against_a_real_server():
    queue = PostgresQueue(PG_DSN)
    await queue.start()
    try:
        url = "https://example.com/real-db-" + __import__("uuid").uuid4().hex
        assert await queue.enqueue(_request(url)) is True
        assert await queue.enqueue(_request(url)) is False

        request = await queue.dequeue()
        assert request.url == url
        assert await queue.complete(request) is True
        assert await queue.is_empty_remote() is True
    finally:
        await queue.close()
