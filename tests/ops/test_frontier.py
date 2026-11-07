from __future__ import annotations

import threading
import time

import pytest

from scrapling_ops.frontier import Frontier, canonical_url, domain_of, url_key


@pytest.fixture()
def frontier(tmp_path):
    f = Frontier(tmp_path / "crawl.db")
    yield f
    f.close()


def test_canonical_url_strips_tracking_and_sorts_query():
    a = canonical_url("HTTPS://Example.COM:443/a/b?utm_source=x&b=2&a=1#frag")
    assert a == "https://example.com/a/b?a=1&b=2"
    assert domain_of(a) == "example.com"


def test_add_is_idempotent(frontier):
    url = "https://example.com/post?utm_campaign=spam"
    assert frontier.add(url) is True
    assert frontier.add("https://example.com/post") is False  # same after canonicalisation
    assert frontier.stats().total == 1


def test_claim_respects_priority(frontier):
    frontier.add("https://example.com/low", priority=1)
    frontier.add("https://example.com/high", priority=9)
    got = frontier.claim("w1", limit=1)
    assert [r.url for r in got] == ["https://example.com/high"]


def test_claim_is_exclusive_across_threads(frontier):
    for i in range(50):
        frontier.add(f"https://example.com/p{i}")
    seen: list[str] = []
    lock = threading.Lock()

    def drain():
        while True:
            got = frontier.claim("w", limit=5, lease=30)
            if not got:
                time.sleep(0.01)
                if frontier.is_empty():
                    return
                continue
            with lock:
                seen.extend(r.url_hash for r in got)
            for r in got:
                frontier.complete(r.url_hash, ok=True)

    threads = [threading.Thread(target=drain) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    assert len(seen) == 50
    assert len(set(seen)) == 50
    assert frontier.stats().done == 50


def test_lease_expiry_is_reaped(frontier):
    frontier.add("https://example.com/x")
    frontier.claim("dead-worker", limit=1, lease=0.05)
    time.sleep(0.1)
    assert frontier.reap() == 1
    again = frontier.claim("alive-worker", limit=1)
    assert len(again) == 1 and again[0].attempts == 1


def test_retry_backoff_then_failed(frontier):
    frontier.add("https://example.com/flaky")
    rec = frontier.claim("w", limit=1)[0]
    state = frontier.complete(rec.url_hash, ok=False, error="429")
    assert state == "pending"

    # still backing off: not claimable right away
    assert frontier.claim("w", limit=1) == []

    row = frontier._conn().execute("SELECT attempts, next_at FROM urls").fetchone()
    assert row["attempts"] == 1
    frontier._conn().execute("UPDATE urls SET next_at=0 WHERE url_hash=?", (rec.url_hash,))

    for _ in range(10):
        claimed = frontier.claim("w", limit=1)
        if not claimed:
            frontier._conn().execute("UPDATE urls SET next_at=0")
            claimed = frontier.claim("w", limit=1)
        if not claimed:
            break
        state = frontier.complete(claimed[0].url_hash, ok=False, error="boom")
        if state == "failed":
            break
        frontier._conn().execute("UPDATE urls SET next_at=0")
    assert frontier.stats().failed == 1


def test_skip_marks_done_without_attempt(frontier):
    frontier.add("https://example.com/private")
    rec = frontier.claim("w", limit=1)[0]
    frontier.skip(rec.url_hash, "deny-pattern")
    stats = frontier.stats()
    assert stats.done == 1 and stats.pending == 0
    assert frontier.get("https://example.com/private").state == "done"


def test_url_key_is_stable_across_spellings():
    assert url_key("https://example.com/a?b=1&a=2") == url_key("https://EXAMPLE.com/a?a=2&b=1&utm_medium=x")


def test_full_width_simhash_round_trip(frontier):
    frontier.add("https://example.com/big")
    rec = frontier.claim("w", limit=1)[0]
    frontier.complete(rec.url_hash, ok=True, simhash=(1 << 64) - 1, content_hash="deadbeef")
    row = frontier._conn().execute("SELECT simhash, content_hash FROM urls").fetchone()
    assert row["simhash"] == "ffffffffffffffff"
    assert row["content_hash"] == "deadbeef"
