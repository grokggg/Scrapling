from __future__ import annotations

import json

from scrapling_ops.frontier import Frontier
from scrapling_ops.guard import DEFAULT_DENY_PATTERNS, ScopeGuard
from scrapling_ops.identity import Identity, IdentityPool
from scrapling_ops.pipeline import FetchResult, JsonlStore, OpsRunner
from scrapling_ops.telemetry import AuditLog, Metrics, redact


PAGES = {
    "https://example.com/": ("Index", ["/a", "/b", "/c", "https://evil.test/x"]),
    "https://example.com/a": ("Article A", []),
    "https://example.com/b": ("Article A", []),  # same text as /a -> content duplicate
    "https://example.com/c": ("Article C", ["/admin/panel"]),
    "https://example.com/admin/panel": ("Admin", []),  # denied by deny-pattern
}


def fake_fetch(url, identity):
    if url not in PAGES:
        return FetchResult(ok=False, status=404, url=url, error="http 404")
    title, _ = PAGES[url]
    from urllib.parse import urljoin

    body = f"<html><head><title>{title}</title></head><body><p>{title} body text with enough words to fingerprint.</p></body></html>"
    return FetchResult(ok=True, status=200, body=body, url=url, elapsed=0.01)


def fake_extract(body, url):
    from urllib.parse import urljoin

    title = body.split("<title>")[1].split("</title>")[0]
    links = PAGES.get(url, ("", []))[1]
    return {"url": url, "title": title, "text": title + " body text with enough words to fingerprint."}, [urljoin(url, l) for l in links]


def test_end_to_end_run(tmp_path):
    items = tmp_path / "items.jsonl"
    runner = OpsRunner(
        Frontier(tmp_path / "crawl.db"),
        ScopeGuard(["example.com"], deny_paths=DEFAULT_DENY_PATTERNS, per_domain_qps=1000),
        IdentityPool([Identity(name="id0")]),
        fake_fetch,
        fake_extract,
        JsonlStore(items),
        workers=3,
        max_pages=20,
        audit=AuditLog(tmp_path / "audit.jsonl"),
        metrics=Metrics(),
        reap_interval=0,
    )
    report = runner.run(["https://example.com/"])
    assert report.pages >= 4
    assert report.denied >= 2          # evil.test link + /admin/panel
    assert report.duplicates >= 1      # /a and /b share text
    assert report.errors == 0

    stored = [json.loads(l) for l in items.read_text(encoding="utf-8").splitlines() if l.strip()]
    assert {i["title"] for i in stored} == {"Index", "Article A", "Article C"}

    audit = AuditLog(tmp_path / "audit.jsonl", redact_payload=False)
    intact, count = audit.verify()
    assert intact and count > 0

    events = [json.loads(l)["event"] for l in (tmp_path / "audit.jsonl").read_text().splitlines()]
    assert {"fetch", "done", "deny"} <= set(events)


def test_metrics_render_prometheus_text(tmp_path):
    metrics = Metrics()
    runner = OpsRunner(
        Frontier(tmp_path / "c.db"),
        ScopeGuard(["example.com"], per_domain_qps=1000),
        IdentityPool([Identity(name="id0")]),
        fake_fetch,
        fake_extract,
        JsonlStore(tmp_path / "i.jsonl"),
        workers=2,
        max_pages=3,
        metrics=metrics,
        reap_interval=0,
    )
    runner.run(["https://example.com/"])
    text = metrics.render()
    assert "crawl_pages_total" in text and "# TYPE crawl_pages_total counter" in text
    assert "crawl_fetch_seconds_bucket" in text


def test_audit_redacts_secrets(tmp_path):
    audit = AuditLog(tmp_path / "a.jsonl")
    audit.write("fetch", url="https://example.com/?u=bob@example.com&t=ghp_ABCDEFGHIJKLMNOPQRSTUV")
    raw = (tmp_path / "a.jsonl").read_text()
    assert "bob@example.com" not in raw and "ghp_" not in raw
    assert AuditLog(tmp_path / "a.jsonl", redact_payload=False).verify()[0]


def test_audit_detects_tampering(tmp_path):
    path = tmp_path / "a.jsonl"
    audit = AuditLog(path, redact_payload=False)
    audit.write("a", n=1)
    audit.write("b", n=2)
    lines = path.read_text().splitlines()
    lines[0] = lines[0].replace('"n": 1', '"n": 9')
    path.write_text("\n".join(lines) + "\n")
    intact, _ = AuditLog(path, redact_payload=False).verify()
    assert not intact


def test_redact_patterns():
    text = "mail bob@example.com phone 13812345678 token ghp_" + "A" * 20 + " id 11010519491231002X"
    out = redact(text)
    for leaked in ("bob@example.com", "13812345678", "ghp_", "11010519491231002X"):
        assert leaked not in out
