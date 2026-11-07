"""Observability primitives: trace ids, JSON logs, and Prometheus-shaped metrics.

No third-party dependency: a handful of counters, a histogram and a formatter are enough to
answer "how many, how fast, how often blocked" without dragging in a metrics SDK.
"""

import json
import logging
import uuid
from collections import defaultdict
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from threading import RLock
from typing import Any, Dict, Iterator, Optional, Tuple

__all__ = [
    "DEFAULT_METRICS",
    "Counter",
    "Gauge",
    "Histogram",
    "MetricsRegistry",
    "StructuredLogFormatter",
    "current_trace_id",
    "new_trace_id",
    "register_defaults",
    "trace_context",
]

NO_TRACE = "-"

_trace_id: ContextVar[str] = ContextVar("scrapling_trace_id", default=NO_TRACE)


# ============ Trace ID ============

def new_trace_id() -> str:
    """A 16-hex id, short enough to paste into a log line."""
    return uuid.uuid4().hex[:16]


def current_trace_id() -> str:
    """The trace id of the current async task or thread, ``"-"`` when there is none."""
    return _trace_id.get()


@contextmanager
def trace_context(trace_id: Optional[str] = None) -> Iterator[str]:
    """Bind a trace id for the duration of the block. Nested blocks restore the outer id."""
    value = trace_id or new_trace_id()
    token = _trace_id.set(value)
    try:
        yield value
    finally:
        _trace_id.reset(token)


# ============ Structured logs ============

class StructuredLogFormatter(logging.Formatter):
    """One JSON object per line: ts, level, logger, msg, trace_id, plus chosen extras."""

    def __init__(self, extra_fields: Tuple[str, ...] = ("domain", "url", "sid")):
        super().__init__()
        self.extra_fields = tuple(extra_fields)

    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "ts": _ts(record.created),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "trace_id": getattr(record, "trace_id", None) or current_trace_id(),
        }
        for name in self.extra_fields:
            value = getattr(record, name, None)
            if value is not None:
                payload[name] = value
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def _ts(created: float) -> str:
    from datetime import datetime, timezone

    return datetime.fromtimestamp(created, tz=timezone.utc).isoformat(timespec="milliseconds")


# ============ Labels ============

LabelKey = str


def _key(labels: Tuple[str, ...], values: Dict[str, Any]) -> LabelKey:
    """Canonical series key: label pairs in declaration order, missing values as empty."""
    for name in values:
        if name not in labels:
            raise ValueError(f"unknown label {name!r}; declared labels are {labels}")
    return ",".join(f'{name}="{_escape(values.get(name, ""))}"' for name in labels)


def _escape(value: Any) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


class _Metric:
    kind = "untyped"

    def __init__(self, name: str, help: str = "", labels: Tuple[str, ...] = ()):
        self.name = name
        self.help = help
        self.labels = tuple(labels)

    def _series(self, labels: Dict[str, Any]) -> LabelKey:
        return _key(self.labels, labels)


class Counter(_Metric):
    """Monotonic counter, one value per label set."""

    kind = "counter"

    def __init__(self, name: str, help: str = "", labels: Tuple[str, ...] = ()):
        super().__init__(name, help, labels)
        self._values: Dict[LabelKey, float] = defaultdict(float)
        self._lock = RLock()

    def inc(self, amount: float = 1.0, **labels: Any) -> None:
        key = self._series(labels)
        with self._lock:
            self._values[key] += amount

    def value(self, **labels: Any) -> float:
        with self._lock:
            return self._values.get(self._series(labels), 0.0)

    def snapshot(self) -> Dict[str, Dict[str, float]]:
        with self._lock:
            return {self.name: dict(self._values)}


class Gauge(_Metric):
    """A value that goes up and down: queue depth, current ladder rung."""

    kind = "gauge"

    def __init__(self, name: str, help: str = "", labels: Tuple[str, ...] = ()):
        super().__init__(name, help, labels)
        self._values: Dict[LabelKey, float] = defaultdict(float)
        self._lock = RLock()

    def set(self, value: float, **labels: Any) -> None:
        key = self._series(labels)
        with self._lock:
            self._values[key] = float(value)

    def inc(self, amount: float = 1.0, **labels: Any) -> None:
        key = self._series(labels)
        with self._lock:
            self._values[key] += amount

    def dec(self, amount: float = 1.0, **labels: Any) -> None:
        self.inc(-amount, **labels)

    def value(self, **labels: Any) -> float:
        with self._lock:
            return self._values.get(self._series(labels), 0.0)

    def snapshot(self) -> Dict[str, Dict[str, float]]:
        with self._lock:
            return {self.name: dict(self._values)}


@dataclass
class _HistogramSeries:
    counts: list = field(default_factory=list)
    total: float = 0.0
    count: int = 0


class Histogram(_Metric):
    """Cumulative buckets, plus sum and count — the Prometheus exposition shape."""

    kind = "histogram"

    def __init__(
        self,
        name: str,
        help: str = "",
        buckets: Tuple[float, ...] = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
        labels: Tuple[str, ...] = (),
    ):
        super().__init__(name, help, labels)
        self.buckets = tuple(sorted(float(b) for b in buckets))
        self._data: Dict[LabelKey, _HistogramSeries] = {}
        self._lock = RLock()

    def observe(self, value: float, **labels: Any) -> None:
        key = self._series(labels)
        with self._lock:
            series = self._series_data(key)
            series.total += value
            series.count += 1
            for index, bound in enumerate(self.buckets):
                if value <= bound:
                    series.counts[index] += 1

    def _series_data(self, key: LabelKey) -> _HistogramSeries:
        series = self._data.get(key)
        if series is None:
            series = _HistogramSeries(counts=[0] * len(self.buckets))
            self._data[key] = series
        return series

    def value(self, **labels: Any) -> Dict[str, Any]:
        with self._lock:
            series = self._data.get(self._series(labels))
            if series is None:
                return {"count": 0, "sum": 0.0, "buckets": {b: 0 for b in self.buckets}}
            return {
                "count": series.count,
                "sum": series.total,
                "buckets": {b: series.counts[i] for i, b in enumerate(self.buckets)},
            }

    def snapshot(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        with self._lock:
            out = {}
            for key, series in self._data.items():
                out[key] = {
                    "buckets": {bound: series.counts[i] for i, bound in enumerate(self.buckets)},
                    "sum": series.total,
                    "count": series.count,
                }
            return {self.name: out}


class MetricsRegistry:
    """Name → metric, and the two exports that matter: a dict and Prometheus text."""

    def __init__(self) -> None:
        self._metrics: Dict[str, _Metric] = {}
        self._lock = RLock()

    def counter(self, name: str, help: str = "", labels: Tuple[str, ...] = ()) -> Counter:
        return self._get(name, Counter, help=help, labels=labels)

    def gauge(self, name: str, help: str = "", labels: Tuple[str, ...] = ()) -> Gauge:
        return self._get(name, Gauge, help=help, labels=labels)

    def histogram(
        self,
        name: str,
        help: str = "",
        buckets: Optional[Tuple[float, ...]] = None,
        labels: Tuple[str, ...] = (),
    ) -> Histogram:
        kwargs = {"help": help, "labels": labels}
        if buckets is not None:
            kwargs["buckets"] = buckets
        return self._get(name, Histogram, **kwargs)

    def _get(self, name: str, cls: type, **kwargs: Any):
        with self._lock:
            existing = self._metrics.get(name)
            if existing is not None:
                if not isinstance(existing, cls):
                    raise TypeError(f"{name} is already a {type(existing).__name__}, not a {cls.__name__}")
                return existing
            metric = cls(name, **kwargs)
            self._metrics[name] = metric
            return metric

    def get(self, name: str) -> Optional[_Metric]:
        with self._lock:
            return self._metrics.get(name)

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            metrics = list(self._metrics.values())
        merged: Dict[str, Any] = {}
        for metric in metrics:
            merged.update(metric.snapshot())
        return merged

    def prometheus_text(self) -> str:
        lines = []
        for metric in list(self._metrics.values()):
            lines.extend(_render(metric))
        return "\n".join(lines) + "\n"

    def __len__(self) -> int:
        return len(self._metrics)


def _render(metric: _Metric) -> Iterator[str]:
    if metric.help:
        yield f"# HELP {metric.name} {metric.help}"
    yield f"# TYPE {metric.name} {metric.kind}"
    data = metric.snapshot().get(metric.name, {})
    if metric.kind == "histogram":
        for key, series in data.items():
            for bound, count in series["buckets"].items():
                yield f'{metric.name}_bucket{_with(key, f"le=\"{bound}\"")} {count}'
            yield f'{metric.name}_bucket{_with(key, "le=\"+Inf\"")} {series["count"]}'
            yield f"{metric.name}_sum{_braces(key)} {series['sum']}"
            yield f"{metric.name}_count{_braces(key)} {series['count']}"
        return
    for key, value in data.items():
        yield f"{metric.name}{_braces(key)} {value}"


def _braces(key: LabelKey) -> str:
    return f"{{{key}}}" if key else ""


def _with(key: LabelKey, extra: str) -> str:
    return f"{{{key},{extra}}}" if key else f"{{{extra}}}"


#: name → (kind, help, extra kwargs). Registered per crawl, not at import time.
DEFAULT_METRICS: Dict[str, Tuple[str, str]] = {
    "scrapling_requests_total": ("counter", "Total requests by status"),
    "scrapling_request_duration_seconds": ("histogram", "Request latency"),
    "scrapling_signals_total": ("counter", "Anti-bot signals detected"),
    "scrapling_retries_total": ("counter", "Retries by decision"),
    "scrapling_proxy_success_total": ("counter", "Proxy success/fail"),
    "scrapling_browser_failures_total": ("counter", "Browser render failures"),
    "scrapling_incremental_hits_total": ("counter", "304 hits"),
    "scrapling_store_ops_total": ("counter", "Store insert/update/dedup_skip"),
    "scrapling_pacing_delay_seconds": ("histogram", "Pacing delay distribution"),
    "scrapling_ladder_level": ("gauge", "Current FetchLevel per domain"),
}

_DEFAULT_LABELS = {
    "scrapling_requests_total": ("status",),
    "scrapling_request_duration_seconds": ("level",),
    "scrapling_signals_total": ("signal",),
    "scrapling_retries_total": ("decision",),
    "scrapling_proxy_success_total": ("result",),
    "scrapling_browser_failures_total": ("exc",),
    "scrapling_incremental_hits_total": (),
    "scrapling_store_ops_total": ("op",),
    "scrapling_pacing_delay_seconds": ("domain",),
    "scrapling_ladder_level": ("domain",),
}


def register_defaults(registry: MetricsRegistry) -> Dict[str, _Metric]:
    """Materialise DEFAULT_METRICS on a registry and hand back name → metric."""
    built: Dict[str, _Metric] = {}
    for name, (kind, help_text) in DEFAULT_METRICS.items():
        labels = _DEFAULT_LABELS.get(name, ())
        if kind == "counter":
            built[name] = registry.counter(name, help_text, labels)
        elif kind == "gauge":
            built[name] = registry.gauge(name, help_text, labels)
        else:
            built[name] = registry.histogram(name, help_text, labels=labels)
    return built
