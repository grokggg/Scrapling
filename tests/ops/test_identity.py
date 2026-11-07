from __future__ import annotations

import pytest

from scrapling_ops.identity import Identity, IdentityPool, NoIdentityAvailable, profiles_from_specs


def pool(n: int = 2) -> IdentityPool:
    return IdentityPool([Identity(name=f"id{i}") for i in range(n)], cooldown_base=60, cooldown_cap=600)


def test_pick_returns_available_identity():
    p = pool()
    assert p.pick().name.startswith("id")


def test_blocked_identity_cools_down_and_is_avoided():
    p = pool(2)
    a = p.pick()
    p.report(a, ok=False, status=403)
    assert not a.available()
    assert a.health < 0.5
    assert all(p.pick().name != a.name for _ in range(10))


def test_all_cooling_down_raises():
    p = pool(1)
    a = p.pick()
    p.report(a, ok=False, status=429)
    with pytest.raises(NoIdentityAvailable):
        p.pick()


def test_success_restores_health():
    p = pool(1)
    a = p.pick()
    p.report(a, ok=False, status=503)
    bad = a.health
    for _ in range(10):
        p.report(a, ok=True, status=200)
    assert a.health > bad and a.health <= 1.0


def test_block_detection_from_body():
    assert IdentityPool.looks_blocked("<h1>Please complete the captcha</h1>")
    assert IdentityPool.looks_blocked("访问过于频繁，请稍后再试")
    assert not IdentityPool.looks_blocked("<html><body>article text</body></html>")


def test_cooldown_grows_exponentially():
    p = pool(1)
    a = p.pick()
    p.report(a, ok=False, status=403)
    first = a.cooldown_until
    a.cooldown_until = 0  # simulate time passing without sleeping
    p.report(a, ok=False, status=403)
    second = a.cooldown_until
    assert second > first


def test_stats_shape():
    p = pool(2)
    p.pick()
    stats = p.stats()
    assert stats["size"] == 2 and len(stats["identities"]) == 2


def test_profiles_from_specs():
    ids = profiles_from_specs([{"name": "a", "headers": {"X-Test": "1"}, "proxy": "http://p:8080", "weight": 2}])
    assert ids[0].name == "a" and ids[0].proxy and ids[0].weight == 2
