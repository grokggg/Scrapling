"""Incremental cache: conditional headers, 304/200 handling, TTL, and both backends."""

import time

import pytest

from scrapling.engines.toolbelt.custom import Response
from scrapling.spiders.incremental import CacheEntry, IncrementalCache
from scrapling.spiders.incremental_storage import MemoryStorage, SQLiteStorage

URL = "https://example.com/page"
BODY = b"<html><body>hello</body></html>"
OTHER = b"<html><body>changed</body></html>"


def make_response(body: bytes = BODY, status: int = 200, headers: dict | None = None, url: str = URL) -> Response:
    return Response(url, body, status, "OK", {}, headers or {}, {}, _log=False)


@pytest.fixture(params=["memory", "sqlite"])
def cache(request):
    storage = MemoryStorage() if request.param == "memory" else SQLiteStorage(":memory:")
    instance = IncrementalCache(storage=storage, ttl_seconds=86400)
    yield instance
    if isinstance(storage, SQLiteStorage):
        storage.close()


class TestRoundTrip:
    def test_put_then_get_returns_the_entry(self, cache):
        entry = cache.put(URL, make_response(headers={"ETag": '"v1"'}))

        got = cache.get(URL)

        assert got == entry
        assert isinstance(got, CacheEntry)
        assert got.url == URL
        assert got.etag == '"v1"'
        assert got.body_sha256
        assert got.body_size == len(BODY)

    def test_an_expired_entry_is_invisible(self, cache):
        stale = CacheEntry(url=URL, stored_at=time.time() - 90000, ttl_seconds=60)
        cache.storage.put(stale, BODY)

        assert cache.get(URL) is None

    def test_delete_removes_the_entry(self, cache):
        cache.put(URL, make_response())

        cache.delete(URL)

        assert cache.get(URL) is None
        assert URL not in cache


class TestConditionalHeaders:
    def test_nothing_cached_means_no_headers(self, cache):
        assert cache.conditional_headers(URL) == {}

    def test_etag_becomes_if_none_match(self, cache):
        cache.put(URL, make_response(headers={"ETag": '"abc123"'}))

        assert cache.conditional_headers(URL) == {"If-None-Match": '"abc123"'}

    def test_last_modified_becomes_if_modified_since(self, cache):
        stamp = "Wed, 21 Oct 2015 07:28:00 GMT"
        cache.put(URL, make_response(headers={"Last-Modified": stamp}))

        assert cache.conditional_headers(URL) == {"If-Modified-Since": stamp}

    def test_both_validators_are_sent(self, cache):
        cache.put(URL, make_response(headers={"etag": '"abc"', "last-modified": "Wed, 21 Oct 2015 07:28:00 GMT"}))

        assert cache.conditional_headers(URL) == {
            "If-None-Match": '"abc"',
            "If-Modified-Since": "Wed, 21 Oct 2015 07:28:00 GMT",
        }


class TestNotModified:
    def test_304_counts_as_unchanged(self, cache):
        cache.put(URL, make_response())

        assert cache.is_not_modified(make_response(body=b"", status=304)) is True

    def test_200_with_the_same_body_counts_as_unchanged(self, cache):
        cache.put(URL, make_response())

        assert cache.is_not_modified(make_response(body=BODY)) is True

    def test_200_with_a_different_body_counts_as_changed(self, cache):
        cache.put(URL, make_response())

        assert cache.is_not_modified(make_response(body=OTHER)) is False

    def test_an_error_is_never_unchanged(self, cache):
        cache.put(URL, make_response())

        assert cache.is_not_modified(make_response(status=500)) is False

    def test_without_a_cached_entry_nothing_matches(self, cache):
        assert cache.is_not_modified(make_response(body=BODY)) is False


class TestRebuild:
    def test_the_cached_body_comes_back_as_a_response(self, cache):
        cache.put(URL, make_response(headers={"ETag": '"v1"'}))

        rebuilt = cache.build_response_from_cache(URL)

        assert rebuilt is not None
        assert rebuilt.status == 200
        assert rebuilt.body == BODY
        assert rebuilt.url == URL

    def test_nothing_cached_means_no_response(self, cache):
        assert cache.build_response_from_cache(URL) is None


class TestStorages:
    def test_the_two_backends_agree(self):
        payload = make_response(headers={"ETag": '"same"'})
        memory, sqlite = MemoryStorage(), SQLiteStorage(":memory:")
        try:
            for storage in (memory, sqlite):
                cache = IncrementalCache(storage=storage, ttl_seconds=60)
                entry = cache.put(URL, payload)
                assert cache.get(URL) == entry
                assert cache.conditional_headers(URL) == {"If-None-Match": '"same"'}
                assert cache.is_not_modified(make_response(body=BODY)) is True
                assert cache.build_response_from_cache(URL).body == BODY
                cache.delete(URL)
                assert cache.get(URL) is None
        finally:
            sqlite.close()

    def test_the_storage_protocol_is_satisfied(self):
        from scrapling.spiders.incremental_storage import IncrementalStorage

        memory, sqlite = MemoryStorage(), SQLiteStorage(":memory:")
        try:
            assert isinstance(memory, IncrementalStorage)
            assert isinstance(sqlite, IncrementalStorage)
        finally:
            sqlite.close()


def test_body_bytes_override_the_response_body(cache):
    entry = cache.put(URL, make_response(), body_bytes=b"decoded")

    assert entry.body_size == len(b"decoded")
    assert cache.build_response_from_cache(URL).body == b"decoded"


def test_a_zero_ttl_entry_never_expires(cache):
    forever = CacheEntry(url=URL, stored_at=0.0, ttl_seconds=0)
    cache.storage.put(forever, BODY)

    assert forever.is_expired() is False
    assert cache.get(URL) is not None
