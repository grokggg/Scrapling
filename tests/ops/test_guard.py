from __future__ import annotations

import time

from scrapling_ops.guard import DEFAULT_DENY_PATTERNS, ScopeGuard


def guard(**kw):
    return ScopeGuard(["example.com"], deny_paths=DEFAULT_DENY_PATTERNS, per_domain_qps=1000, **kw)


def test_off_domain_is_denied():
    g = guard()
    d = g.check("https://evil.test/x")
    assert not d and d.reason == "off-domain"


def test_subdomain_is_in_scope():
    g = guard()
    assert g.check("https://docs.example.com/guide").allowed
    assert not g.check("https://notexample.com/x").allowed


def test_deny_patterns_block_account_and_destructive_paths():
    g = guard()
    assert not g.check("https://example.com/admin/users").allowed
    assert not g.check("https://example.com/item?delete=1").allowed
    assert not g.check("https://example.com/.env").allowed
    assert g.check("https://example.com/blog/post-1").allowed


def test_max_depth():
    g = guard(max_depth=2)
    assert g.check("https://example.com/a", depth=2).allowed
    assert g.check("https://example.com/a", depth=3).reason == "max-depth"


def test_rate_limit_returns_wait_time():
    g = ScopeGuard(["example.com"], per_domain_qps=1, per_domain_burst=1)
    assert g.check("https://example.com/1").allowed
    second = g.check("https://example.com/2")
    assert not second and second.reason == "rate-limit"
    assert 0 < second.wait <= 1.5


def test_daily_budget():
    g = ScopeGuard(["example.com"], per_domain_qps=1000, daily_budget=2)
    assert g.check("https://example.com/1").allowed
    assert g.check("https://example.com/2").allowed
    assert g.check("https://example.com/3").reason == "daily-budget"


def test_robots_hook_is_consulted():
    g = ScopeGuard(["example.com"], robots_allowed=lambda url: "/private" not in url)
    assert g.check("https://example.com/ok").allowed
    assert g.check("https://example.com/private/x").reason == "robots"


def test_https_requirement():
    g = ScopeGuard(["example.com"], require_https=True)
    assert g.check("http://example.com/x").reason == "https-required"


def test_stats_are_counted():
    g = guard()
    g.check("https://example.com/a")
    g.check("https://other.test/b")
    s = g.stats()
    assert s["checks"] == 2 and s["denied"] == 1
    assert s["domains"] == {"example.com": 1}


def test_empty_scope_is_rejected():
    try:
        ScopeGuard([])
    except ValueError as exc:
        assert "allowed domain" in str(exc)
    else:
        raise AssertionError("empty scope must not be allowed")
