"""Proxy pool: scoring, cooling, death, domain affinity, probing, thread safety."""

import threading

import pytest

from scrapling.spiders.proxy_pool import ProxyEntry, ProxyPool, ProxyPoolConfig, ProxyState

A = "http://user:pass@10.0.0.1:8000"
B = "http://user:pass@10.0.0.2:8000"
C = "http://user:pass@10.0.0.3:8000"


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def pool() -> ProxyPool:
    return ProxyPool()


def _fails(pool: ProxyPool, url: str, times: int, domain: str = "") -> None:
    for _ in range(times):
        pool.release(url, success=False, domain=domain)


class TestInventory:
    def test_add_remove_and_stats(self, pool):
        pool.add_many([A, B])
        assert len(pool) == 2
        assert A in pool

        stats = pool.stats()
        assert stats == {"total": 2, "healthy": 2, "degraded": 0, "cooling": 0, "dead": 0, "avg_score": 100.0}

        pool.remove(A)
        assert len(pool) == 1 and A not in pool

    def test_adding_twice_keeps_the_original_entry(self, pool):
        pool.add(A)
        pool.get(A).score = 40.0

        pool.add(A)

        assert pool.get(A).score == 40.0

    def test_empty_url_is_rejected(self, pool):
        with pytest.raises(ValueError):
            pool.add("")

    def test_remove_unknown_url_is_a_no_op(self, pool):
        pool.remove("http://nowhere:1")
        assert len(pool) == 0

    def test_get_unknown_returns_none(self, pool):
        assert pool.get("http://nowhere:1") is None


class TestAcquire:
    def test_empty_pool_returns_none(self, pool):
        assert pool.acquire("example.com") is None

    def test_prefers_the_higher_score(self, pool):
        pool.add_many([A, B])
        pool.get(B).score = 50.0

        picks = [pool.acquire() for _ in range(1000)]

        assert picks.count(A) > 600
        assert set(picks) == {A, B}

    def test_skips_a_cooling_proxy(self, pool):
        pool.add(A)
        _fails(pool, A, 3)

        assert pool.get(A).state is ProxyState.COOLING
        assert pool.acquire() is None

    def test_skips_a_dead_proxy(self, pool):
        pool.add(A)
        _fails(pool, A, 5)

        assert pool.get(A).state is ProxyState.DEAD
        assert pool.acquire() is None
        assert pool.stats()["dead"] == 1

    def test_skips_a_proxy_below_min_score(self, pool):
        pool.add(A)
        pool.get(A).score = 19.0

        assert pool.acquire() is None

    def test_records_last_used(self, pool):
        clock = _Clock()
        pool = ProxyPool(clock=clock)
        pool.add(A)

        pool.acquire()

        assert pool.get(A).last_used_at == clock.now


class TestScoring:
    def test_success_adds_five(self, pool):
        pool.add(A)

        pool.release(A, success=True, latency_ms=120)

        entry = pool.get(A)
        assert entry.score == 100.0  # capped
        assert entry.success_count == 1
        assert entry.avg_latency_ms == 120

    def test_fail_subtracts_fifteen(self, pool):
        pool.add(A)

        pool.release(A, success=False)

        entry = pool.get(A)
        assert entry.score == 85.0
        assert entry.fail_count == 1
        assert entry.state is ProxyState.DEGRADED

    def test_success_resets_the_consecutive_counter(self, pool):
        pool.add(A)
        _fails(pool, A, 2)
        assert pool.get(A).consecutive_fail == 2

        pool.release(A, success=True)

        entry = pool.get(A)
        assert entry.consecutive_fail == 0
        assert entry.state is ProxyState.HEALTHY

    def test_latency_is_an_ema(self, pool):
        pool.add(A)
        pool.release(A, success=True, latency_ms=100)
        pool.release(A, success=True, latency_ms=200)

        assert pool.get(A).avg_latency_ms == pytest.approx(130.0)  # 0.7*100 + 0.3*200

    def test_release_of_an_unknown_url_is_ignored(self, pool):
        pool.release("http://nowhere:1", success=False)
        assert pool.stats()["total"] == 0


class TestStateMachine:
    def test_three_consecutive_failures_start_a_cooldown(self):
        clock = _Clock()
        pool = ProxyPool(ProxyPoolConfig(cooling_seconds=300), clock=clock)
        pool.add(A)

        _fails(pool, A, 3)

        entry = pool.get(A)
        assert entry.state is ProxyState.COOLING
        assert entry.cooling_until == clock.now + 300

    def test_a_cooling_proxy_returns_as_degraded(self):
        clock = _Clock()
        pool = ProxyPool(ProxyPoolConfig(cooling_seconds=300), clock=clock)
        pool.add(A)
        _fails(pool, A, 3)

        clock.advance(301)
        assert pool.acquire() == A

        entry = pool.get(A)
        assert entry.state is ProxyState.DEGRADED
        assert entry.consecutive_fail == 0
        assert entry.score == 55.0  # 100 - 3*15, floor is min_score

    def test_five_failures_kill_the_proxy(self):
        clock = _Clock()
        pool = ProxyPool(clock=clock)
        pool.add(A)

        _fails(pool, A, 5)
        clock.advance(10_000)

        assert pool.get(A).state is ProxyState.DEAD
        assert pool.acquire() is None  # no revival, not even after the cooldown

    def test_cooling_score_floor_is_min_score(self):
        clock = _Clock()
        pool = ProxyPool(ProxyPoolConfig(cooling_seconds=10, min_score=20.0), clock=clock)
        pool.add(A)
        _fails(pool, A, 3)
        pool.get(A).score = 5.0  # pretend the score fell below the floor

        clock.advance(11)
        pool.acquire()

        assert pool.get(A).score == 20.0
        assert pool.get(A).state is ProxyState.DEGRADED


class TestDomainAffinity:
    def test_bonus_values(self, pool):
        from scrapling.spiders.proxy_pool import _domain_bonus

        entry = ProxyEntry(A)
        assert _domain_bonus(entry, "example.com") == 0.0  # no history

        entry.domain_stats["example.com"] = {"success": 9, "fail": 1}
        assert _domain_bonus(entry, "example.com") == 0.5

        entry.domain_stats["example.com"] = {"success": 1, "fail": 9}
        assert _domain_bonus(entry, "example.com") == -0.5

        entry.domain_stats["example.com"] = {"success": 5, "fail": 5}
        assert _domain_bonus(entry, "example.com") == 0.0

        assert _domain_bonus(entry, "") == 0.0

    def test_a_trusted_proxy_wins_the_domain(self, pool):
        pool.add_many([A, B])
        pool.get(A).domain_stats["example.com"] = {"success": 10, "fail": 0}
        pool.get(B).domain_stats["example.com"] = {"success": 1, "fail": 9}
        pool.get(B).score = 30.0  # 100 for A, 30 for B: 150 vs 15 in weighted terms

        picks = [pool.acquire("example.com") for _ in range(1000)]

        # weights are 150 vs 15 here, so A takes ~91% of the draws with room for noise
        assert picks.count(A) > 800

    def test_domain_history_is_recorded_on_release(self, pool):
        pool.add(A)

        pool.release(A, success=True, domain="example.com")
        pool.release(A, success=False, domain="example.com")

        assert pool.get(A).domain_stats["example.com"] == {"success": 1, "fail": 1}


class TestHealthCheck:
    @pytest.mark.asyncio
    async def test_probe_marks_the_unreachable_one(self):
        def checker(url: str) -> bool:
            return url != B

        pool = ProxyPool(health_checker=checker)
        pool.add_many([A, B, C])

        results = await pool.health_check_all()

        assert results == {A: True, B: False, C: True}
        assert pool.get(A).state is ProxyState.HEALTHY
        assert pool.get(B).state is ProxyState.DEGRADED
        assert pool.get(B).score == 85.0
        assert pool.get(B).consecutive_fail == 1
        assert pool.get(B).last_checked_at > 0

    @pytest.mark.asyncio
    async def test_async_checker_is_awaited(self):
        async def checker(url: str) -> bool:
            return False

        pool = ProxyPool(health_checker=checker)
        pool.add(A)

        results = await pool.health_check_all()

        assert results == {A: False}
        assert pool.get(A).state is ProxyState.DEGRADED

    @pytest.mark.asyncio
    async def test_without_a_checker_nothing_is_probed(self, pool):
        pool.add(A)

        assert await pool.health_check_all() == {}
        assert pool.get(A).state is ProxyState.HEALTHY
        assert pool.get(A).last_checked_at == 0.0

    @pytest.mark.asyncio
    async def test_dead_proxies_are_not_probed(self):
        calls = []
        pool = ProxyPool(health_checker=lambda url: calls.append(url) or True)
        pool.add_many([A, B])
        _fails(pool, A, 5)

        results = await pool.health_check_all()

        assert results == {B: True}
        assert calls == [B]


class TestThreadSafety:
    def test_concurrent_acquire_and_release_keep_the_books(self, pool):
        pool.add_many([A, B, C])
        errors = []

        def hammer(index: int) -> None:
            try:
                for step in range(50):
                    url = pool.acquire("example.com")
                    if url is None:
                        continue
                    pool.release(url, success=(index + step) % 2 == 0, domain="example.com")
            except Exception as exc:  # pragma: no cover - only on a real race
                errors.append(exc)

        threads = [threading.Thread(target=hammer, args=(i,)) for i in range(10)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        stats = pool.stats()
        assert errors == []
        assert stats["total"] == 3
        assert stats["healthy"] + stats["degraded"] + stats["cooling"] + stats["dead"] == 3
        assert all(0.0 <= pool.get(url).score <= 100.0 for url in (A, B, C))
        assert sum(pool.get(url).success_count + pool.get(url).fail_count for url in (A, B, C)) > 0
