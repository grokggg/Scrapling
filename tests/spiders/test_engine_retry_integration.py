"""Engine ↔ retry policy integration: status codes, exceptions, and the two paths.

The fetch layer is replaced by a scripted session, so no network is touched.
Delays stay small because the guard spider's policy has a 0.1s floor.
"""

import logging

import pytest

from scrapling.engines.toolbelt.custom import Response
from scrapling.spiders.engine import CrawlerEngine
from scrapling.spiders.request import Request
from scrapling.spiders.retry import RetryPolicy
from scrapling.spiders.session import SessionManager
from scrapling.spiders.spider import Spider


def _response(url: str, status: int) -> Response:
    return Response(url, b"<html><body>probe</body></html>", status, "probe", {}, {}, {}, _log=False)


class _ScriptedSession:
    """Returns the scripted outcomes in order, then repeats the last one."""

    def __init__(self, *outcomes):
        self.outcomes = list(outcomes)
        self.calls = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def fetch(self, url: str, **kwargs) -> Response:
        index = min(self.calls, len(self.outcomes) - 1)
        self.calls += 1
        outcome = self.outcomes[index]
        if isinstance(outcome, BaseException):
            raise outcome
        return _response(url, outcome)


class _GuardSpider(Spider):
    """Real spider: exercises get_retry_policy/_bump_retry, with a 0.1s delay floor."""

    name = "engine_retry_guard"
    retry_policy = RetryPolicy(base_delay=0.0001, jitter=0.0)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.errors = []

    async def parse(self, response):
        yield {"url": response.url}

    async def on_error(self, request, error) -> None:
        self.errors.append((request, error))


class _HooklessSpider:
    """Duck-typed spider without get_retry_policy/_bump_retry (pre-existing spiders)."""

    name = "engine_retry_hookless"
    concurrent_requests = 2
    concurrent_requests_per_domain = 0
    download_delay = 0.0
    max_blocked_retries = 3
    autothrottle_enabled = False
    autothrottle_start_delay = 5.0
    autothrottle_max_delay = 60.0
    autothrottle_target_concurrency = None
    autothrottle_block_backoff = True
    development_mode = False
    development_cache_dir = None
    allowed_domains: set = set()
    robots_txt_obey = False
    fp_include_kwargs = False
    fp_include_headers = False
    fp_keep_fragments = False
    start_urls: list = []

    async def is_blocked(self, response) -> bool:
        return False


def _engine(spider, *outcomes) -> CrawlerEngine:
    manager = SessionManager()
    manager.add("default", _ScriptedSession(*outcomes))
    return CrawlerEngine(spider, manager)


def _request(attempt: int = 0, **kwargs) -> Request:
    return Request("https://example.com/page", sid="default", _retry_count=attempt, **kwargs)


async def _only_queued(engine: CrawlerEngine) -> Request:
    assert not engine.scheduler.is_empty, "expected a retry to be scheduled"
    return await engine.scheduler.dequeue()


class TestStatusRetries:
    @pytest.mark.asyncio
    async def test_503_is_retried_with_higher_priority(self):
        engine = _engine(_GuardSpider(), 503)
        request = _request()

        await engine._process_request(request)

        retry = await _only_queued(engine)
        assert retry.url == request.url
        assert retry.priority == -1
        assert retry._retry_count == 1
        assert retry.dont_filter is True
        assert request._retry_count == 0

    @pytest.mark.asyncio
    async def test_503_on_later_attempt_keeps_priority(self):
        engine = _engine(_GuardSpider(), 503)
        request = _request(attempt=1)

        await engine._process_request(request)

        retry = await _only_queued(engine)
        assert retry.priority == 0
        assert retry._retry_count == 2

    @pytest.mark.asyncio
    async def test_429_rotates_the_proxy(self):
        engine = _engine(_GuardSpider(), 429)
        request = _request(proxy="http://127.0.0.1:8080")

        await engine._process_request(request)

        retry = await _only_queued(engine)
        assert "proxy" not in retry._session_kwargs
        assert request._session_kwargs["proxy"] == "http://127.0.0.1:8080"
        assert retry.priority == 0

    @pytest.mark.asyncio
    async def test_404_is_not_retried(self):
        engine = _engine(_GuardSpider(), 404)

        await engine._process_request(_request())

        assert engine.scheduler.is_empty
        assert engine.stats.response_status_count["status_404"] == 1

    @pytest.mark.asyncio
    async def test_exhausted_budget_warns_and_stops(self, caplog):
        caplog.set_level(logging.WARNING, logger="scrapling")
        engine = _engine(_GuardSpider(), 503)

        with caplog.at_level(logging.WARNING, logger="scrapling"):
            await engine._process_request(_request(attempt=5))

        assert engine.scheduler.is_empty
        assert "retry exhausted" in caplog.text
        assert "status=503" in caplog.text
        assert "attempt=5" in caplog.text

    @pytest.mark.asyncio
    async def test_non_retryable_status_is_silent(self, caplog):
        caplog.set_level(logging.WARNING, logger="scrapling")
        engine = _engine(_GuardSpider(), 404)

        with caplog.at_level(logging.WARNING, logger="scrapling"):
            await engine._process_request(_request())

        assert "retry exhausted" not in caplog.text
        assert "retry give up" not in caplog.text


class TestExceptionRetries:
    @pytest.mark.asyncio
    async def test_timeout_is_retried(self):
        engine = _engine(_GuardSpider(), TimeoutError("slow"))

        await engine._process_request(_request())

        retry = await _only_queued(engine)
        assert retry._retry_count == 1
        assert retry.dont_filter is True

    @pytest.mark.asyncio
    async def test_value_error_fails_without_retry(self):
        spider = _GuardSpider()
        engine = _engine(spider, ValueError("bad"))

        await engine._process_request(_request())

        assert engine.scheduler.is_empty
        assert engine.stats.failed_requests_count == 1
        assert len(spider.errors) == 1
        assert isinstance(spider.errors[0][1], ValueError)

    @pytest.mark.asyncio
    async def test_give_up_is_logged(self, caplog):
        caplog.set_level(logging.WARNING, logger="scrapling")
        engine = _engine(_GuardSpider(), ValueError("bad"))

        with caplog.at_level(logging.WARNING, logger="scrapling"):
            await engine._process_request(_request(attempt=2))

        assert "retry give up" in caplog.text
        assert "ValueError" in caplog.text
        assert "attempt=2" in caplog.text


class TestSpiderWithoutHooks:
    @pytest.mark.asyncio
    async def test_missing_policy_and_bumper_fall_back_to_defaults(self):
        engine = _engine(_HooklessSpider(), 503)
        request = _request()

        await engine._process_request(request)

        retry = await _only_queued(engine)
        assert retry._retry_count == 1
        assert retry.priority == -1
        assert retry.dont_filter is True
