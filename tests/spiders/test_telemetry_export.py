"""The scrape endpoint: real sockets, real HTTP, no mocks."""

import socket
import urllib.error
import urllib.request

import pytest

from scrapling.spiders.telemetry import MetricsRegistry
from scrapling.spiders.telemetry_export import TelemetryExporter


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _get(url: str) -> tuple[int, str]:
    with urllib.request.urlopen(url, timeout=5) as response:
        return response.status, response.read().decode()


@pytest.fixture
def registry() -> MetricsRegistry:
    instance = MetricsRegistry()
    instance.counter("scrapling_requests_total", "Total requests", labels=("status",)).inc(3, status="200")
    instance.gauge("scrapling_ladder_level", "Rung", labels=("domain",)).set(2, domain="a.com")
    return instance


def test_metrics_are_served_over_http(registry):
    exporter = TelemetryExporter(registry, port=_free_port())
    exporter.start()
    try:
        status, body = _get(exporter.url())

        assert status == 200
        assert "# TYPE scrapling_requests_total counter" in body
        assert 'scrapling_requests_total{status="200"} 3.0' in body
        assert 'scrapling_ladder_level{domain="a.com"} 2.0' in body
    finally:
        exporter.stop()


def test_the_root_tells_you_where_to_scrape(registry):
    exporter = TelemetryExporter(registry, port=_free_port())
    exporter.start()
    try:
        status, body = _get(exporter.url().replace("/metrics", "/"))

        assert status == 200
        assert "/metrics" in body
    finally:
        exporter.stop()


def test_an_unknown_path_is_a_404(registry):
    exporter = TelemetryExporter(registry, port=_free_port())
    exporter.start()
    try:
        with pytest.raises(urllib.error.HTTPError) as caught:
            _get(exporter.url().replace("/metrics", "/nope"))

        assert caught.value.code == 404
    finally:
        exporter.stop()


def test_stop_closes_the_socket(registry):
    exporter = TelemetryExporter(registry, port=_free_port())
    exporter.start()
    url = exporter.url()
    exporter.stop()

    assert exporter.url() is None
    assert exporter.running is False
    with pytest.raises(urllib.error.URLError):
        _get(url)


def test_a_zero_port_never_listens(registry):
    exporter = TelemetryExporter(registry, port=0)

    exporter.start()

    assert exporter.running is False
    assert exporter.url() is None
    exporter.stop()  # always safe


def test_start_and_stop_are_idempotent(registry):
    exporter = TelemetryExporter(registry, port=_free_port())

    exporter.start()
    first = exporter.url()
    exporter.start()
    exporter.stop()
    exporter.stop()

    assert first == first
    assert exporter.url() is None


def test_the_context_manager_cleans_up(registry):
    with TelemetryExporter(registry, port=_free_port()) as exporter:
        assert _get(exporter.url())[0] == 200
        url = exporter.url()

    with pytest.raises(urllib.error.URLError):
        _get(url)
