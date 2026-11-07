"""Store contract: upsert per URL, content index, pagination, thread safety, close semantics."""

import threading

import pytest

from scrapling.spiders.store import StoreClosed, StoredItem, content_hash, url_hash
from scrapling.spiders.stores import MemoryStore, SQLiteStore


@pytest.fixture(params=["memory", "sqlite"])
def store(request):
    instance = MemoryStore() if request.param == "memory" else SQLiteStore(":memory:")
    yield instance
    instance.close()


def item(url: str = "https://example.com/a", title: str = "first", **extra) -> StoredItem:
    return StoredItem(url=url, item={"title": title, **extra})


class TestBasics:
    def test_an_empty_store_counts_zero(self, store):
        assert store.count() == 0

    def test_a_new_item_is_reported_as_new(self, store):
        assert store.put(item()) is True
        assert store.count() == 1

    def test_the_same_url_updates_instead_of_duplicating(self, store):
        store.put(item(title="first"))

        assert store.put(item(title="second")) is False
        assert store.count() == 1
        assert store.get("https://example.com/a").item["title"] == "second"

    def test_get_returns_the_stored_item(self, store):
        written = item(price=9.5)
        store.put(written)

        read = store.get("https://example.com/a")

        assert read.url == written.url
        assert read.url_hash == written.url_hash
        assert read.content_hash == written.content_hash
        assert read.item == {"title": "first", "price": 9.5}
        assert read.fetched_at == written.fetched_at

    def test_get_of_an_unknown_url_is_none(self, store):
        assert store.get("https://example.com/missing") is None

    def test_delete_reports_whether_it_removed_something(self, store):
        store.put(item())

        assert store.delete("https://example.com/a") is True
        assert store.count() == 0
        assert store.delete("https://example.com/a") is False


class TestPagination:
    def test_iter_items_walks_in_stable_pages(self, store):
        for index in range(5):
            store.put(item(url=f"https://example.com/{index}"))

        page = list(store.iter_items(limit=2, offset=0))
        rest = list(store.iter_items(limit=2, offset=2))

        assert len(page) == 2
        assert len(rest) == 2
        assert {row.url for row in page}.isdisjoint({row.url for row in rest})
        assert len(list(store.iter_items(limit=100, offset=0))) == 5


class TestFields:
    def test_meta_survives_a_round_trip(self, store):
        store.put(
            StoredItem(
                url="https://example.com/a",
                item={"title": "x"},
                meta={"lang": "zh", "depth": 2},
                source_sid="tls:2",
            )
        )

        read = store.get("https://example.com/a")

        assert read.meta == {"lang": "zh", "depth": 2}
        assert read.source_sid == "tls:2"

    def test_hashes_are_derived_not_guessed(self):
        row = item()

        assert row.url_hash == url_hash("https://example.com/a")
        assert row.content_hash == content_hash({"title": "first"})
        assert row.fetched_at > 0

    def test_key_order_does_not_change_the_content_hash(self):
        assert content_hash({"a": 1, "b": 2}) == content_hash({"b": 2, "a": 1})


class TestConcurrency:
    def test_ten_threads_writing_one_url_leave_one_row(self, store):
        errors = []

        def writer():
            try:
                store.put(item(title="burst"))
            except Exception as exc:  # pragma: no cover - only on a real regression
                errors.append(exc)

        threads = [threading.Thread(target=writer) for _ in range(10)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert errors == []
        assert store.count() == 1


class TestContentIndex:
    def test_two_urls_with_the_same_content_are_findable(self, store):
        store.put(item(url="https://example.com/a", title="same"))
        store.put(item(url="https://example.com/b", title="same"))
        digest = content_hash({"title": "same"})

        found = store.find_by_content(digest)

        assert len(found) == 2
        assert store.has_content(digest) is True
        assert store.has_content(content_hash({"title": "other"})) is False


class TestClose:
    def test_using_a_closed_store_raises(self, store):
        store.close()

        with pytest.raises(StoreClosed):
            store.count()


class TestBatch:
    def test_put_many_splits_new_from_updated(self, store):
        store.put(item(url="https://example.com/a", title="old"))

        new, updated = store.put_many([
            item(url="https://example.com/a", title="new"),
            item(url="https://example.com/b"),
            item(url="https://example.com/c"),
        ])

        assert (new, updated) == (2, 1)
        assert store.count() == 3
        assert store.get("https://example.com/a").item["title"] == "new"
