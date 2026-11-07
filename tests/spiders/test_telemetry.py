"""Metrics, trace ids and the JSON log formatter."""

import json
import logging
import re

import pytest

from scrapling.spiders.telemetry import (
    Counter,
    Gauge,
    Histogram,
    MetricsRegistry,
    StructuredLogFormatter,
    current_trace_id,
    new_trace_id,
    register_defaults,
    trace_context,
)


class TestCounter:
    def test_inc_accumulates(self):
        counter = Counter("c_total")

        counter.inc()
        counter.inc(2.5)

        assert counter.value() == 3.5

    def test_each_label_set_counts_alone(self):
        counter = Counter("c_total", labels=("status",))

        counter.inc(status="200")
        counter.inc(status="200")
        counter.inc(status="500")

        assert counter.value(status="200") == 2
        assert counter.value(status="500") == 1
        assert counter.value(status="404") == 0

    def test_an_undeclared_label_is_rejected(self):
        with pytest.raises(ValueError, match="unknown label"):
            Counter("c_total", labels=("status",)).inc(code="200")

    def test_snapshot_is_keyed_by_series(self):
        counter = Counter("c_total", labels=("status",))
        counter.inc(3, status="200")

        assert counter.snapshot() == {"c_total": {'status="200"': 3.0}}


class TestGauge:
    def test_set_inc_dec(self):
        gauge = Gauge("g")

        gauge.set(5)
        gauge.inc(2)
        gauge.dec()
        assert gauge.value() == 6

        gauge.set(0)
        assert gauge.value() == 0

    def test_labels_are_independent(self):
        gauge = Gauge("g", labels=("domain",))

        gauge.set(3, domain="a")
        gauge.set(7, domain="b")
        gauge.dec(2, domain="b")

        assert gauge.snapshot() == {"g": {'domain="a"': 3.0, 'domain="b"': 5.0}}


class TestHistogram:
    def test_observations_fill_buckets_sum_and_count(self):
        histogram = Histogram("h", buckets=(0.1, 1.0))
        for value in (0.05, 0.5, 5.0):
            histogram.observe(value)

        series = histogram.value()

        assert series["buckets"] == {0.1: 1, 1.0: 2}, "buckets are cumulative"
        assert series["count"] == 3
        assert series["sum"] == pytest.approx(5.55)

    def test_labels_split_the_series(self):
        histogram = Histogram("h", buckets=(1.0,), labels=("level",))
        histogram.observe(0.5, level="tls")
        histogram.observe(2.0, level="browser")

        assert histogram.value(level="tls")["count"] == 1
        assert histogram.value(level="browser")["buckets"][1.0] == 0

    def test_buckets_are_sorted_on_construction(self):
        assert Histogram("h", buckets=(1.0, 0.1)).buckets == (0.1, 1.0)


class TestRegistry:
    def test_the_same_name_returns_the_same_instance(self):
        registry = MetricsRegistry()

        assert registry.counter("a_total") is registry.counter("a_total")

    def test_a_type_conflict_is_loud(self):
        registry = MetricsRegistry()
        registry.counter("x")

        with pytest.raises(TypeError, match="already a Counter"):
            registry.gauge("x")

    def test_snapshot_merges_every_metric(self):
        registry = MetricsRegistry()
        registry.counter("c_total").inc(2)
        registry.gauge("g").set(1)
        registry.histogram("h", buckets=(1.0,)).observe(0.5)

        snapshot = registry.snapshot()

        assert set(snapshot) == {"c_total", "g", "h"}
        assert snapshot["h"][""]["count"] == 1

    def test_prometheus_text_has_help_type_and_values(self):
        registry = MetricsRegistry()
        registry.counter("req_total", "Requests", labels=("status",)).inc(4, status="200")
        registry.gauge("level", "Rung", labels=("domain",)).set(3, domain="a")
        registry.histogram("lat", "Latency", buckets=(0.5,)).observe(0.2)

        text = registry.prometheus_text()
        lines = text.strip().splitlines()

        assert "# HELP req_total Requests" in lines
        assert "# TYPE req_total counter" in lines
        assert 'req_total{status="200"} 4.0' in lines
        assert "# TYPE level gauge" in lines
        assert 'level{domain="a"} 3.0' in lines
        assert "# TYPE lat histogram" in lines
        assert 'lat_bucket{le="0.5"} 1' in lines
        assert 'lat_bucket{le="+Inf"} 1' in lines
        assert "lat_sum 0.2" in lines
        assert "lat_count 1" in lines
        assert text.endswith("\n")
        assert re.search(r'^# TYPE \w+ (counter|gauge|histogram)$', text, re.MULTILINE)

    def test_label_values_are_escaped(self):
        registry = MetricsRegistry()
        registry.counter("c", labels=("path",)).inc(path='a"b\\c')

        assert 'path="a\\"b\\\\c"' in registry.prometheus_text()

    def test_register_defaults_builds_the_whole_set(self):
        registry = MetricsRegistry()

        metrics = register_defaults(registry)

        assert len(metrics) == 10
        assert metrics["scrapling_requests_total"] is registry.get("scrapling_requests_total")
        assert metrics["scrapling_ladder_level"].kind == "gauge"


class TestTraceId:
    def test_ids_are_unique(self):
        ids = {new_trace_id() for _ in range(1000)}

        assert len(ids) == 1000
        assert all(len(value) == 16 and int(value, 16) >= 0 for value in ids)

    def test_the_context_exposes_the_id_and_restores_the_outside(self):
        assert current_trace_id() == "-"

        with trace_context() as inner:
            assert current_trace_id() == inner
            with trace_context("fixed"):
                assert current_trace_id() == "fixed"
            assert current_trace_id() == inner

        assert current_trace_id() == "-"

    def test_an_explicit_id_is_kept(self):
        with trace_context("abc123") as value:
            assert value == "abc123"


class TestStructuredLogFormatter:
    def _record(self, **extra) -> logging.LogRecord:
        record = logging.LogRecord("scrapling.x", logging.INFO, __file__, 10, "hello %s", ("world",), None)
        for key, value in extra.items():
            setattr(record, key, value)
        return record

    def test_the_line_is_json_with_the_documented_fields(self):
        payload = json.loads(StructuredLogFormatter().format(self._record()))

        assert payload["level"] == "INFO"
        assert payload["logger"] == "scrapling.x"
        assert payload["msg"] == "hello world"
        assert payload["trace_id"] == "-"
        assert re.match(r"\d{4}-\d{2}-\d{2}T", payload["ts"])

    def test_the_trace_id_comes_from_the_context(self):
        with trace_context("deadbeef"):
            payload = json.loads(StructuredLogFormatter().format(self._record()))

        assert payload["trace_id"] == "deadbeef"

    def test_chosen_extras_are_promoted_and_others_dropped(self):
        record = self._record(domain="a.com", url="https://a.com", sid="tls:2", secret="no")

        payload = json.loads(StructuredLogFormatter().format(record))

        assert payload["domain"] == "a.com"
        assert payload["url"] == "https://a.com"
        assert payload["sid"] == "tls:2"
        assert "secret" not in payload

    def test_narrowing_the_extra_fields_works(self):
        record = self._record(domain="a.com", sid="tls:2")

        payload = json.loads(StructuredLogFormatter(extra_fields=("domain",)).format(record))

        assert payload["domain"] == "a.com"
        assert "sid" not in payload

    def test_an_exception_is_rendered(self):
        try:
            raise ValueError("boom")
        except ValueError:
            import sys

            record = logging.LogRecord(
                "scrapling.x", logging.ERROR, __file__, 10, "failed", (), sys.exc_info()
            )

        payload = json.loads(StructuredLogFormatter().format(record))

        assert "ValueError: boom" in payload["exc"]
