"""Engine → telemetry wiring: registry, trace ids, and every counter the pipeline can emit."""

import logging
import socket
import urllib.request

import pytest

from scrapling.spiders.adaptive import FetchLevel
from scrapling.spiders.engine import CrawlerEngine
from scrapling.spiders.incremental import IncrementalCache
from scrapling.spiders.incremental_storage import MemoryStorage
from scrapling.spiders.request import Request
from scrapling.spiders.result import CrawlStats
from scrapling.spiders.session import SessionManager
from scrapling.spiders.spider import Spider
from scrapling.spiders.stores import MemoryStore
from scrapling.spiders.telemetry import DEFAULT_METRICS, current_trace_id

URL = "https://example.com/a"


class _Response:
    """Just enough of a Response for the engine's bookkeeping."""

    def __init__(self, status: int = 200, body: bytes = b"<html>ok</html>", url: str = URL, headers=None):
        self.status = status
        self.body = body
        self.url = url
        self.encoding = "utf-8"
        self.headers = headers or {}
        self.meta = {}
        self.request = None

    def __str__(self) -> str:
        return self.url


class _Session:
    """A fake session: answers with a scripted response per call, or raises."""

    name = "fake"

    def __init__(self, *answers) -> None:
        self._answers = list(answers) or [_Response()]
        self._is_alive = False
        self.calls = 0

    async def __aenter__(self):
        self._is_alive = True
        return self

    async def __aexit__(self, *exc_info):
        self._is_alive = False

    async def fetch(self, url, **kwargs):
        self.calls += 1
        answer = self._answers.pop(0) if len(self._answers) > 1 else self._answers[0]
        if isinstance(answer, Exception):
            raise answer
        return answer


class _Spider(Spider):
    name = "telemetry_probe"
    start_urls = [URL]
    traces = []

    async def parse(self, response):
        type(self).traces.append(current_trace_id())
        yield {"url": getattr(response, "url", URL)}


class _Plan:
    """Adaptive stand-in: a fixed rung per domain, so the gauge can be checked."""

    def __init__(self, levels: dict) -> None:
        self.levels = levels

    def plan(self, domain):
        return (FetchLevel(self.levels.get(domain, 0)), 0.0)

    def should_reset(self, domain):
        return False

    def observe(self, *args, **kwargs):
        return None

    def observe_success(self, domain):
        return None

    def reset_session(self, domain):
        return None


class _Pool:
    def __init__(self, proxy: str | None = "http://10.0.0.1:8080") -> None:
        self.proxy = proxy
        self.releases = []

    def acquire(self, domain):
        return self.proxy

    def release(self, proxy, success=True, latency_ms=0.0, domain=""):
        self.releases.append(success)


def _engine(spider: Spider, session: _Session, *, port: int = 0) -> CrawlerEngine:
    spider.telemetry_enabled = True
    spider.telemetry_export_port = port
    manager = SessionManager()
    manager.add("default", session)
    engine = CrawlerEngine(spider, manager)
    engine._per_domain_sessions = False
    engine.session_manager.ensure_domain_session = lambda domain, level: "default"
    return engine


async def _run(engine: CrawlerEngine, url: str = URL) -> None:
    await engine._process_request(Request(url))


@pytest.mark.asyncio
async def test_telemetry_off_costs_nothing():
    _Spider.traces = []
    spider = _Spider()
    spider.telemetry_enabled = False
    manager = SessionManager()
    manager.add("default", _Session())
    engine = CrawlerEngine(spider, manager)

    await _run(engine)

    assert engine._metrics is None
    assert engine._exporter is None
    assert _Spider.traces == ["-"], "no trace context is entered when telemetry is off"


@pytest.mark.asyncio
async def test_telemetry_on_registers_every_default_metric():
    _Spider.traces = []
    engine = _engine(_Spider(), _Session())

    await _run(engine)

    assert engine._metrics is not None
    assert len(engine._metrics) == len(DEFAULT_METRICS) == 10
    assert set(engine._metrics.snapshot()) == set(DEFAULT_METRICS)


@pytest.mark.asyncio
async def test_a_successful_fetch_counts_by_status():
    engine = _engine(_Spider(), _Session(_Response(status=200)))

    await _run(engine)

    assert engine._metrics.get("scrapling_requests_total").value(status="200") == 1
    assert engine._metrics.get("scrapling_request_duration_seconds").value(level="DIRECT")["count"] == 1


@pytest.mark.asyncio
async def test_a_failed_fetch_counts_as_error():
    engine = _engine(_Spider(), _Session(TimeoutError("gone")))

    await _run(engine)

    assert engine._metrics.get("scrapling_requests_total").value(status="error") == 1
    decisions = engine._metrics.get("scrapling_retries_total").snapshot()["scrapling_retries_total"]
    assert sum(decisions.values()) == 1, decisions


@pytest.mark.asyncio
async def test_retry_decisions_are_counted_by_name():
    engine = _engine(_Spider(), _Session(TimeoutError("slow")))

    await _run(engine)

    decisions = engine._metrics.get("scrapling_retries_total").snapshot()["scrapling_retries_total"]
    assert list(decisions) == ['decision="RETRY_SAME_PRIORITY"'], decisions
    assert decisions['decision="RETRY_SAME_PRIORITY"'] == 1


@pytest.mark.asyncio
async def test_a_block_signal_is_counted():
    engine = _engine(_Spider(), _Session(_Response(status=403, body=b"forbidden")))
    engine._adaptive_engine = _Plan({})

    await _run(engine)

    snapshot = engine._metrics.get("scrapling_signals_total").snapshot()["scrapling_signals_total"]
    assert any(key.startswith('signal="FORBIDDEN"') for key in snapshot), snapshot


@pytest.mark.asyncio
async def test_proxy_outcomes_are_counted():
    engine = _engine(_Spider(), _Session(_Response(status=200)))
    engine._per_domain_sessions = True
    engine._adaptive_engine = _Plan({"example.com": FetchLevel.PROXY.value})
    engine._proxy_pool = _Pool()

    await _run(engine)

    assert engine._metrics.get("scrapling_proxy_success_total").value(result="success") == 1


@pytest.mark.asyncio
async def test_a_browser_fallback_is_counted_once():
    engine = _engine(_Spider(), _Session())
    engine._per_domain_sessions = True
    engine._adaptive_engine = _Plan({"example.com": FetchLevel.BROWSER.value})
    engine.session_manager.fetch = _Session(TimeoutError("page.goto"), _Response(status=200)).fetch

    await _run(engine)

    assert engine._metrics.get("scrapling_browser_failures_total").value(exc="TimeoutError") == 1
    assert engine._metrics.get("scrapling_requests_total").value(status="200") == 1
    assert engine._metrics.get("scrapling_requests_total").snapshot()["scrapling_requests_total"] == {'status="200"': 1.0}


@pytest.mark.asyncio
async def test_an_incremental_hit_is_counted():
    spider = _Spider()
    spider.incremental_cache = IncrementalCache(MemoryStorage(), ttl_seconds=3600)
    spider.incremental_cache.put(URL, _Response(status=200, headers={"ETag": '"v1"'}))
    engine = _engine(spider, _Session(_Response(status=304, body=b"")))

    await _run(engine)

    assert engine._metrics.get("scrapling_incremental_hits_total").value() == 1


@pytest.mark.asyncio
async def test_store_operations_are_counted_by_kind():
    spider = _Spider()
    spider.store = MemoryStore()
    engine = _engine(spider, _Session(_Response(status=200)))

    await _run(engine)
    await _run(engine)

    ops = engine._metrics.get("scrapling_store_ops_total")
    assert ops.value(op="new") == 1
    assert ops.value(op="update") == 1


@pytest.mark.asyncio
async def test_a_pacing_delay_is_observed():
    class _Pacing:
        def next_delay(self, domain):
            return 0.0

        def record_request(self, domain):
            return None

        def should_rotate(self, domain):
            return False

    engine = _engine(_Spider(), _Session())
    engine._pacing = _Pacing()

    await _run(engine)

    assert engine._metrics.get("scrapling_pacing_delay_seconds").value(domain="example.com")["count"] == 1


@pytest.mark.asyncio
async def test_the_ladder_gauge_is_per_domain():
    engine = _engine(_Spider(), _Session())
    engine._per_domain_sessions = True
    engine._adaptive_engine = _Plan({"a.com": FetchLevel.DIRECT.value, "b.com": FetchLevel.TLS.value})
    engine.session_manager.fetch = _Session(_Response(status=200)).fetch

    await _run(engine, "https://a.com/1")
    await _run(engine, "https://b.com/1")

    gauge = engine._metrics.get("scrapling_ladder_level")
    assert gauge.value(domain="a.com") == float(FetchLevel.DIRECT.value)
    assert gauge.value(domain="b.com") == float(FetchLevel.TLS.value)


@pytest.mark.asyncio
async def test_the_trace_id_lives_for_one_request():
    _Spider.traces = []
    engine = _engine(_Spider(), _Session())

    await _run(engine)

    inside = _Spider.traces[0]
    assert inside != "-"
    assert len(inside) == 16 and int(inside, 16) >= 0
    assert current_trace_id() == "-", "the context is gone once the request is done"


@pytest.mark.asyncio
async def test_the_exporter_runs_for_exactly_the_crawl(tmp_path):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]

    seen = []

    class _CrawlSpider(_Spider):
        name = "telemetry_crawl"

        async def on_start(self, resuming: bool = False):
            url = engine._exporter.url()
            with urllib.request.urlopen(url, timeout=5) as response:
                seen.append((response.status, response.read().decode()))

        async def start_requests(self):
            yield Request(URL)

    spider = _CrawlSpider()
    engine = _engine(spider, _Session(_Response(status=200)), port=port)

    stats = await engine.crawl()

    assert isinstance(stats, CrawlStats)
    assert engine._exporter.running is False, "the endpoint must not outlive the crawl"
    assert seen and seen[0][0] == 200
    assert "scrapling_requests_total" in seen[0][1]


@pytest.mark.asyncio
async def test_no_exporter_is_started_without_a_port():
    class _CrawlSpider(_Spider):
        name = "telemetry_no_port"

        async def start_requests(self):
            yield Request(URL)

    engine = _engine(_CrawlSpider(), _Session(_Response(status=200)))

    await engine.crawl()

    assert engine._metrics is not None, "metrics still collect"
    assert engine._exporter is None, "port=0 means nothing listens"


@pytest.mark.asyncio
async def test_json_logs_only_touch_the_spiders_own_logger():
    spider = _Spider()
    spider.telemetry_log_format = "json"
    engine = _engine(spider, _Session())
    logger = logging.getLogger("scrapling.spiders.telemetry_probe")
    before = list(logger.handlers)

    try:
        engine._install_json_logs()
        handlers = [h for h in logger.handlers if h.get_name() == "scrapling-telemetry"]

        assert handlers and handlers[0].formatter.__class__.__name__ == "StructuredLogFormatter"
        assert logger.propagate is False
        assert logging.getLogger("scrapling.spiders.other").propagate is True
    finally:
        logger.handlers = before
        logger.propagate = True

@pytest.mark.asyncio
async def test_fallback_success_counts_one_request_no_error():
    """A request rescued by the TLS fallback counts once, with its final status."""
    spider = _Spider()
    spider.browser_fallback_to_static = True
    engine = _engine(spider, _Session())
    engine._per_domain_sessions = True
    engine._adaptive_engine = _Plan({"example.com": FetchLevel.BROWSER.value})
    engine.session_manager.fetch = _Session(TimeoutError("page.goto"), _Response(status=200)).fetch

    await _run(engine)

    requests_total = engine._metrics.get("scrapling_requests_total").snapshot()["scrapling_requests_total"]
    assert requests_total == {'status="200"': 1.0}, requests_total
    assert 'status="error"' not in requests_total
    assert engine.stats.requests_count == 1
    assert engine._metrics.get("scrapling_browser_failures_total").value(exc="TimeoutError") == 1


@pytest.mark.asyncio
async def test_fallback_failure_counts_one_error():
    """Browser fails and the fallback fails too: one request, one error, one retry decision."""
    spider = _Spider()
    spider.browser_fallback_to_static = True
    engine = _engine(spider, _Session())
    engine._per_domain_sessions = True
    engine._adaptive_engine = _Plan({"example.com": FetchLevel.BROWSER.value})
    engine.session_manager.fetch = _Session(TimeoutError("page.goto"), RuntimeError("tls gone")).fetch

    await _run(engine)

    requests_total = engine._metrics.get("scrapling_requests_total").snapshot()["scrapling_requests_total"]
    assert requests_total == {'status="error"': 1.0}, requests_total
    decisions = engine._metrics.get("scrapling_retries_total").snapshot()["scrapling_retries_total"]
    assert sum(decisions.values()) == 1, decisions
    assert engine._metrics.get("scrapling_browser_failures_total").value(exc="TimeoutError") == 1


@pytest.mark.asyncio
async def test_non_browser_failure_counts_one_error():
    """A failure on a static rung never enters the fallback block, so it counts once as error."""
    spider = _Spider()
    spider.browser_fallback_to_static = True
    engine = _engine(spider, _Session(TimeoutError("connect")))
    engine._per_domain_sessions = True
    engine._adaptive_engine = _Plan({"example.com": FetchLevel.TLS.value})

    await _run(engine)

    requests_total = engine._metrics.get("scrapling_requests_total").snapshot()["scrapling_requests_total"]
    assert requests_total == {'status="error"': 1.0}, requests_total
    assert engine._metrics.get("scrapling_browser_failures_total").snapshot()["scrapling_browser_failures_total"] == {}

@pytest.mark.asyncio
async def test_ladder_level_reflects_served_level_after_fallback():
    """The gauge must end on the rung that served the page, not the one that failed."""
    spider = _Spider()
    spider.browser_fallback_to_static = True
    engine = _engine(spider, _Session())
    engine._per_domain_sessions = True
    engine._adaptive_engine = _Plan({"example.com": FetchLevel.BROWSER.value})
    engine.session_manager.fetch = _Session(TimeoutError("page.goto"), _Response(status=200)).fetch

    await _run(engine)

    gauge = engine._metrics.get("scrapling_ladder_level")
    assert gauge.value(domain="example.com") == float(FetchLevel.TLS) == 2.0
    assert gauge.value(domain="example.com") != float(FetchLevel.BROWSER)


@pytest.mark.asyncio
async def test_ladder_level_unchanged_without_fallback():
    """No fallback, no gauge write beyond the entry level."""
    engine = _engine(_Spider(), _Session(_Response(status=200)))

    await _run(engine)

    assert engine._metrics.get("scrapling_ladder_level").value(domain="example.com") == 0.0


@pytest.mark.asyncio
async def test_browser_failures_counted_when_fallback_disabled():
    """With the switch off nothing rescues a browser failure, but the failure is still visible."""
    spider = _Spider()
    spider.browser_fallback_to_static = False
    engine = _engine(spider, _Session())
    engine._per_domain_sessions = True
    engine._adaptive_engine = _Plan({"example.com": FetchLevel.BROWSER.value})
    engine.session_manager.fetch = _Session(TimeoutError("page.goto")).fetch

    await _run(engine)

    assert engine._metrics.get("scrapling_browser_failures_total").value(exc="TimeoutError") == 1
    assert engine._metrics.get("scrapling_requests_total").snapshot()["scrapling_requests_total"] == {'status="error"': 1.0}
    durations = engine._metrics.get("scrapling_request_duration_seconds").snapshot()["scrapling_request_duration_seconds"]
    assert not any('level="TLS"' in key for key in durations), durations
