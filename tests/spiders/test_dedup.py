"""Content dedup: window semantics, store fallback, FIFO eviction."""

import pytest

from scrapling.spiders.dedup import ContentDeduplicator
from scrapling.spiders.store import StoredItem
from scrapling.spiders.stores import MemoryStore, SQLiteStore


@pytest.fixture(params=["memory", "sqlite"])
def store(request):
    instance = MemoryStore() if request.param == "memory" else SQLiteStore(":memory:")
    yield instance
    instance.close()


def item(url: str = "https://example.com/a", title: str = "first") -> StoredItem:
    return StoredItem(url=url, item={"title": title})


def test_nothing_is_a_duplicate_at_the_start(store):
    assert ContentDeduplicator(store).is_duplicate(item()) is False


def test_a_seen_item_is_a_duplicate(store):
    dedup = ContentDeduplicator(store)
    seen = item()

    dedup.mark_seen(seen)

    assert dedup.is_duplicate(seen) is True
    assert len(dedup) == 1


def test_different_content_is_not_a_duplicate(store):
    dedup = ContentDeduplicator(store)
    dedup.mark_seen(item(title="first"))

    assert dedup.is_duplicate(item(title="second")) is False


def test_the_window_evicts_the_oldest_first(store):
    dedup = ContentDeduplicator(store, window=2)
    first, second, third = item(title="one"), item(title="two"), item(title="three")
    for row in (first, second):
        dedup.mark_seen(row)

    dedup.mark_seen(third)

    assert len(dedup) == 2
    assert dedup.is_duplicate(third) is True
    assert dedup.is_duplicate(second) is True
    assert dedup.is_duplicate(first) is False, "evicted from the window, and never stored"


def test_the_store_is_the_backstop_after_eviction(store):
    dedup = ContentDeduplicator(store, window=1)
    old = item(title="already-stored")
    store.put(old)  # written by an earlier run, no longer in the window

    dedup.mark_seen(item(title="something-else"))

    assert dedup.is_duplicate(old) is True, "the store index must catch what the window forgot"


def test_forget_hands_the_decision_back_to_the_store(store):
    dedup = ContentDeduplicator(store)
    row = item()
    store.put(row)
    dedup.mark_seen(row)

    dedup.forget(row)

    assert len(dedup) == 0


def test_accept_stores_once_and_reports_the_second_time(store):
    dedup = ContentDeduplicator(store)
    row = item()

    assert dedup.accept(row) is True
    assert dedup.accept(row) is False
    assert store.count() == 1


def test_clear_empties_the_window(store):
    dedup = ContentDeduplicator(store)
    dedup.mark_seen(item())

    dedup.clear()

    assert len(dedup) == 0
