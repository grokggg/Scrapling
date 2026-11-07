"""Tests for Spider-level retry plumbing: policy lookup, counter bump, dict isolation.

No engine, no network — only the spider's own contract.
"""

from types import SimpleNamespace

from scrapling.spiders.request import Request
from scrapling.spiders.retry import RetryPolicy
from scrapling.spiders.spider import Spider


class _ProbeSpider(Spider):
    """Concrete spider with no overrides."""

    name = "retry_probe"

    async def parse(self, response):
        yield None


class _OverrideSpider(Spider):
    """Concrete spider that declares a default policy and one host override."""

    name = "retry_probe_override"
    retry_policy = RetryPolicy(max_attempts=9)
    retry_policy_overrides = {"example.com": RetryPolicy(max_attempts=7)}

    async def parse(self, response):
        yield None


def _req(url: str, attempt: int = 0) -> Request:
    return Request(url, _retry_count=attempt)


class TestGetRetryPolicy:
    def test_defaults_when_nothing_is_configured(self):
        spider = _ProbeSpider()
        assert spider.get_retry_policy(_req("https://example.com/a")) == RetryPolicy()

    def test_custom_policy_without_overrides(self):
        spider = _OverrideSpider()
        assert spider.get_retry_policy(_req("https://other.test/a")) == RetryPolicy(max_attempts=9)
        assert spider.get_retry_policy(_req("https://other.test/a")).max_attempts == 9

    def test_exact_host_match_returns_the_override(self):
        spider = _OverrideSpider()
        policy = spider.get_retry_policy(_req("https://example.com/a"))
        assert policy is _OverrideSpider.retry_policy_overrides["example.com"]
        assert policy.max_attempts == 7

    def test_subdomain_match_returns_the_override(self):
        spider = _ProbeSpider()
        override = RetryPolicy(max_attempts=2)
        spider.retry_policy_overrides = {"example.com": override}
        assert spider.get_retry_policy(_req("https://sub.example.com/a")) is override

    def test_non_matching_host_falls_back_to_default(self):
        spider = _ProbeSpider()
        spider.retry_policy_overrides = {"example.com": RetryPolicy(max_attempts=2)}
        # "notexample.com" must not be treated as a subdomain of "example.com".
        assert spider.get_retry_policy(_req("https://notexample.com/a")) == RetryPolicy()

    def test_url_without_host_falls_back_to_default(self):
        spider = _ProbeSpider()
        assert spider.get_retry_policy(_req("file:///tmp/page.html")) == RetryPolicy()


class TestBumpRetry:
    def test_bump_increments_from_zero(self):
        spider = _ProbeSpider()
        request = _req("https://example.com/a")
        assert request._retry_count == 0
        spider._bump_retry(request)
        assert request._retry_count == 1
        spider._bump_retry(request)
        assert request._retry_count == 2

    def test_bump_starts_from_zero_when_the_field_is_missing(self):
        spider = _ProbeSpider()
        request = SimpleNamespace(url="https://example.com/a")
        spider._bump_retry(request)
        assert request._retry_count == 1


class TestOverridesIsolation:
    def test_instances_do_not_share_the_dict(self):
        first, second = _ProbeSpider(), _ProbeSpider()
        assert first.retry_policy_overrides is not second.retry_policy_overrides

        first.retry_policy_overrides["example.com"] = RetryPolicy(max_attempts=2)

        assert "example.com" not in second.retry_policy_overrides
        assert _ProbeSpider.retry_policy_overrides == {}
        assert first.get_retry_policy(_req("https://example.com/a")).max_attempts == 2
        assert second.get_retry_policy(_req("https://example.com/a")) == RetryPolicy()

    def test_subclass_declaration_is_inherited_and_stays_class_level(self):
        spider = _OverrideSpider()
        assert "example.com" in spider.retry_policy_overrides
        assert spider.get_retry_policy(_req("https://example.com/a")).max_attempts == 7

        spider.retry_policy_overrides.pop("example.com")

        assert "example.com" in _OverrideSpider.retry_policy_overrides
        assert spider.get_retry_policy(_req("https://example.com/a")).max_attempts == 9
