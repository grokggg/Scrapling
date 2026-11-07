"""PostgresStore against a fake pool: the Python side is exercised without asyncpg or a server."""

import asyncio
from contextlib import asynccontextmanager

import pytest

from scrapling.spiders.store import StoreClosed, StoredItem, content_hash, url_hash
from scrapling.spiders.stores.postgres import PostgresStore


class _Transaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False


class _Conn:
    def __init__(self, rows: dict) -> None:
        self.rows = rows
        self.statements = []

    async def fetchval(self, query, *args):
        self.statements.append(("fetchval", query, args))
        if "COUNT(*)" in query:
            return len(self.rows)
        if "SELECT 1" in query and "content_hash" in query:
            return 1 if any(row["content_hash"] == args[0] for row in self.rows.values()) else None
        if "SELECT 1" in query:
            return 1 if args[0] in self.rows else None
        return None

    async def fetchrow(self, query, *args):
        self.statements.append(("fetchrow", query, args))
        row = self.rows.get(args[0])
        return row

    async def fetch(self, query, *args):
        self.statements.append(("fetch", query, args))
        if "content_hash = $1" in query:
            return [row for row in self.rows.values() if row["content_hash"] == args[0]]
        if "ANY($1" in query:
            return [{"url_hash": key} for key in args[0] if key in self.rows]
        return list(self.rows.values())[: args[0]]

    async def execute(self, query, *args):
        self.statements.append(("execute", query, args))
        if query.strip().startswith("DELETE"):
            return "DELETE 1" if self.rows.pop(args[0], None) is not None else "DELETE 0"
        if "INSERT INTO" in query:
            self.rows[args[0]] = {
                "url_hash": args[0],
                "url": args[1],
                "content_hash": args[2],
                "item": args[3],
                "fetched_at": args[4],
                "source_sid": args[5],
                "meta": args[6],
            }
            return "INSERT 0 1"
        return "CREATE TABLE"

    async def executemany(self, query, args_list):
        self.statements.append(("executemany", query, len(args_list)))
        for args in args_list:
            await self.execute(query, *args)

    def transaction(self):
        return _Transaction()


class _Pool:
    def __init__(self) -> None:
        self.conn = _Conn({})
        self.closed = False

    @asynccontextmanager
    async def acquire(self):
        yield self.conn

    async def close(self):
        self.closed = True


def _row(url: str = "https://example.com/a", title: str = "first") -> StoredItem:
    return StoredItem(url=url, item={"title": title}, meta={"lang": "zh"}, source_sid="tls:2")


@pytest.fixture
def store():
    pool = _Pool()
    return PostgresStore(pool=pool)


def test_put_then_get_round_trips(store):
    row = _row()

    assert asyncio.run(store.put(row)) is True
    read = asyncio.run(store.get("https://example.com/a"))

    assert read.url_hash == row.url_hash
    assert read.item == {"title": "first"}
    assert read.meta == {"lang": "zh"}
    assert read.source_sid == "tls:2"


def test_a_second_put_updates(store):
    asyncio.run(store.put(_row()))

    assert asyncio.run(store.put(_row(title="second"))) is False
    assert asyncio.run(store.count()) == 1


def test_get_of_an_unknown_url_is_none(store):
    assert asyncio.run(store.get("https://example.com/missing")) is None


def test_delete_reports_what_it_did(store):
    asyncio.run(store.put(_row()))

    assert asyncio.run(store.delete("https://example.com/a")) is True
    assert asyncio.run(store.delete("https://example.com/a")) is False


def test_content_lookup_uses_the_index(store):
    asyncio.run(store.put(_row(url="https://example.com/a", title="same")))
    asyncio.run(store.put(_row(url="https://example.com/b", title="same")))
    digest = content_hash({"title": "same"})

    found = asyncio.run(store.find_by_content(digest))

    assert len(found) == 2
    assert asyncio.run(store.has_content(digest)) is True
    assert asyncio.run(store.has_content(content_hash({"title": "other"}))) is False


def test_iter_items_pages(store):
    for index in range(3):
        asyncio.run(store.put(_row(url=f"https://example.com/{index}")))

    async def collect():
        return [row async for row in store.iter_items(limit=2, offset=1)]

    rows = asyncio.run(collect())

    assert len(rows) == 2


def test_put_many_splits_new_from_updated(store):
    asyncio.run(store.put(_row(url="https://example.com/a", title="old")))

    new, updated = asyncio.run(
        store.put_many(
            [_row(url="https://example.com/a", title="new"), _row(url="https://example.com/b")]
        )
    )

    assert (new, updated) == (1, 1)
    assert asyncio.run(store.count()) == 2


def test_a_closed_store_refuses_work(store):
    asyncio.run(store.close())

    with pytest.raises(StoreClosed):
        asyncio.run(store.count())


def test_url_and_content_hashes_match_the_shared_helpers(store):
    row = _row()

    assert row.url_hash == url_hash("https://example.com/a")
    assert row.content_hash == content_hash({"title": "first"})
