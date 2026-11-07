"""Metrics + tamper-evident audit log.

Two different consumers:

* `Metrics` — Prometheus text format, scraped by whatever you already run.
  Counters, gauges and histograms with labels; no third-party dependency.
* `AuditLog` — append-only JSONL, one line per request decision. This is the
  artefact you hand over when someone asks "what exactly did this crawl touch?".
  It is written with `O_APPEND` and a per-line `\n`, so a crash loses at most the
  last partial line; every record carries a chained hash so gaps are detectable.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import time
from collections import defaultdict
from pathlib import Path
from typing import Iterable, Optional

DEFAULT_BUCKETS = (0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60)

_EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
_PHONE_RE = re.compile(r"(?<!\d)(?:\+?86[- ]?)?1[3-9]\d{9}(?!\d)")
_TOKEN_RE = re.compile(r"\b(gh[pousr]_[A-Za-z0-9]{16,}|sk-[A-Za-z0-9]{16,}|AKIA[0-9A-Z]{16}|eyJ[\w-]{10,}\.[\w-]{10,}\.[\w-]{10,})")
_HEX_RE = re.compile(r"\b[a-f0-9]{32,}\b", re.I)
_ID_CARD_RE = re.compile(r"(?<!\d)\d{17}[\dXx](?!\d)")


def redact(text: str) -> str:
    """Mask the identifiers that must never land in a log or a dataset."""
    if not text:
        return text
    text = _TOKEN_RE.sub("[REDACTED_TOKEN]", text)
    text = _EMAIL_RE.sub("[REDACTED_EMAIL]", text)
    text = _ID_CARD_RE.sub("[REDACTED_ID]", text)
    text = _PHONE_RE.sub("[REDACTED_PHONE]", text)
    text = _HEX_RE.sub("[REDACTED_HASH]", text)
    return text


def _fmt_labels(labels: dict[str, str]) -> str:
    if not labels:
        return ""
    body = ",".join(f'{k}="{str(v).replace(chr(34), chr(39))}"' for k, v in sorted(labels.items()))
    return "{" + body + "}"


class Metrics:
    def __init__(self, *, prefix: str = "crawl"):
        self.prefix = prefix
        self._counters: dict[tuple[str, tuple], float] = defaultdict(float)
        self._gauges: dict[tuple[str, tuple], float] = {}
        self._hist: dict[tuple[str, tuple], list[float]] = defaultdict(list)
        self._help: dict[str, str] = {}
        self._lock = threading.Lock()

    def describe(self, name: str, help_text: str) -> None:
        self._help[name] = help_text

    def inc(self, name: str, value: float = 1.0, **labels) -> None:
        with self._lock:
            self._counters[(name, tuple(sorted(labels.items())))] += value

    def set(self, name: str, value: float, **labels) -> None:
        with self._lock:
            self._gauges[(name, tuple(sorted(labels.items())))] = value

    def observe(self, name: str, value: float, **labels) -> None:
        with self._lock:
            self._hist[(name, tuple(sorted(labels.items())))].append(value)

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "counters": {f"{n}{_fmt_labels(dict(l))}": v for (n, l), v in self._counters.items()},
                "gauges": {f"{n}{_fmt_labels(dict(l))}": v for (n, l), v in self._gauges.items()},
                "histograms": {f"{n}{_fmt_labels(dict(l))}": {"count": len(v), "sum": round(sum(v), 3)} for (n, l), v in self._hist.items()},
            }

    def render(self, *, buckets: Iterable[float] = DEFAULT_BUCKETS) -> str:
        """Prometheus text exposition format."""
        buckets = tuple(buckets)
        lines: list[str] = []
        with self._lock:
            for (name, labels), value in sorted(self._counters.items()):
                full = f"{self.prefix}_{name}"
                lines.append(f"# TYPE {full} counter")
                lines.append(f"{full}{_fmt_labels(dict(labels))} {value:g}")
            for (name, labels), value in sorted(self._gauges.items()):
                full = f"{self.prefix}_{name}"
                lines.append(f"# TYPE {full} gauge")
                lines.append(f"{full}{_fmt_labels(dict(labels))} {value:g}")
            for (name, labels), values in sorted(self._hist.items()):
                full = f"{self.prefix}_{name}"
                lines.append(f"# TYPE {full} histogram")
                label_dict = dict(labels)
                cumulative = 0
                for bound in buckets:
                    cumulative = sum(1 for v in values if v <= bound)
                    lab = dict(label_dict, le=f"{bound:g}")
                    lines.append(f"{full}_bucket{_fmt_labels(lab)} {cumulative}")
                lab = dict(label_dict, le="+Inf")
                lines.append(f"{full}_bucket{_fmt_labels(lab)} {len(values)}")
                lines.append(f"{full}_sum{_fmt_labels(label_dict)} {sum(values):g}")
                lines.append(f"{full}_count{_fmt_labels(label_dict)} {len(values)}")
        return "\n".join(lines) + ("\n" if lines else "")


class AuditLog:
    """Append-only JSONL with a hash chain (`prev` field) so deletions are visible."""

    def __init__(self, path: str | Path, *, redact_payload: bool = True):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.redact_payload = redact_payload
        self._lock = threading.Lock()
        self._prev = self._last_hash()

    def _last_hash(self) -> str:
        if not self.path.exists():
            return "genesis"
        try:
            with self.path.open("rb") as handle:
                handle.seek(max(0, self.path.stat().st_size - 8192))
                tail = handle.read().decode("utf-8", "ignore").strip().splitlines()
            if not tail:
                return "genesis"
            return json.loads(tail[-1]).get("hash", "genesis")
        except Exception:
            return "genesis"

    def write(self, event: str, **fields) -> dict:
        record = {"ts": time.time(), "event": event, **fields, "prev": self._prev}
        if self.redact_payload:
            for key, value in list(record.items()):
                if isinstance(value, str) and key not in ("event", "prev", "hash"):
                    record[key] = redact(value)
        blob = json.dumps(record, ensure_ascii=False, sort_keys=True).encode("utf-8")
        record["hash"] = hashlib.sha256(blob + self._prev.encode()).hexdigest()[:32]
        line = json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n"
        with self._lock:
            fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            try:
                os.write(fd, line.encode("utf-8"))
            finally:
                os.close(fd)
            self._prev = record["hash"]
        return record

    def verify(self) -> tuple[bool, int]:
        """Walk the chain. Returns (intact, records_checked)."""
        prev = "genesis"
        count = 0
        if not self.path.exists():
            return True, 0
        with self.path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                declared = rec.pop("hash", None)
                blob = json.dumps(rec, ensure_ascii=False, sort_keys=True).encode("utf-8")
                expected = hashlib.sha256(blob + prev.encode()).hexdigest()[:32]
                if declared != expected:
                    return False, count
                prev = declared
                count += 1
        return True, count


def utc_stamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
