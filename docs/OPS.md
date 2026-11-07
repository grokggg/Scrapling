# scrapling-ops — crawl operations layer

> 中文速览：这是给 Scrapling 加的一层"作业层"。单机库已经有了（抓取、解析、自适应选择器），
> 缺的是**多 worker 共享任务、崩溃可续、按内容去重、身份健康度、作用域闸门、可审计日志、Prometheus 指标**。
> 本包补的就是这七件。所有设计以"**只在授权范围内采集**"为前提，闸门不是装饰，是契约。

Scrapling gives you a great *single* fetch — stealthy engines, adaptive selectors, a spider with
throttle and checkpoints. What it does not give you is the operational shell around a **long-running,
multi-worker, resumable, auditable** collection job. That is what `scrapling_ops` adds.

```
             ┌──────────── guard ────────────┐
 seed ──► frontier (SQLite/WAL) ──► worker pool ──► fetch (Scrapling) ──► extract ──► dedupe ──► store
             ▲        │ lease/retry            │                                    │
             │        ▼                        ▼                                    ▼
          reaper   checkpoints            identity pool                         audit.jsonl
```

## Why each piece exists

| Problem in a real crawl | What breaks without it | What this package does |
|---|---|---|
| Two crawlers, one job | Duplicate fetches, split stats | **`Frontier`**: SQLite WAL, `BEGIN IMMEDIATE` lease, priority queue, at-least-once |
| Worker killed mid-run | Lost work / stuck URLs | Lease expiry + `reap()`; URL returns to `pending` with `attempts+1` |
| Same article at 5 URLs | Store full of duplicates | **`NearDupIndex`**: SimHash-64 + banded lookup, content-level dedupe |
| One blocked profile kills everything | Whole run 403s | **`IdentityPool`**: health scores, exponential cooldown, ban detection |
| A link walk wanders onto admin pages | Incident | **`ScopeGuard`**: allow-list, deny regexes, depth, QPS bucket, daily budget, robots hook |
| "What did this crawl actually touch?" | No answer | **`AuditLog`**: append-only JSONL with a hash chain (`audit-verify` detects edits) |
| Nobody knows it's running | Silent failure | **`Metrics`**: Prometheus exposition for pages/items/dupes/denies/latency |

## Quick start

```bash
pip install -e .            # Scrapling + deps
python -m scrapling_ops.cli run \
  --seed https://example.com/blog \
  --allow example.com \
  --max-pages 500 --workers 8 --qps 1 \
  --db crawl.db --out items.jsonl --audit audit.jsonl --metrics-out metrics.txt \
  --stealth

python -m scrapling_ops.cli status --db crawl.db
python -m scrapling_ops.cli audit-verify --path audit.jsonl
```

Resume an interrupted run — same `--db`, add `--resume` (no re-seed):

```bash
python -m scrapling_ops.cli run --seed https://example.com/blog --allow example.com --resume --db crawl.db
```

Programmatic use:

```python
from scrapling_ops import Frontier, ScopeGuard, IdentityPool, Identity
from scrapling_ops.pipeline import OpsRunner, JsonlStore
from scrapling_ops.adapters import scrapling_fetcher, generic_extractor

runner = OpsRunner(
    Frontier("crawl.db"),
    ScopeGuard(["example.com"], per_domain_qps=2, max_depth=4),
    IdentityPool([Identity(name="a"), Identity(name="b")]),
    scrapling_fetcher(stealth=False),
    generic_extractor(),
    JsonlStore("items.jsonl"),
    workers=8,
)
print(runner.run(["https://example.com/blog"]).as_dict())
```

## Design contracts

1. **The guard is the single authority.** Every URL entering the frontier is re-checked at claim
   time, including redirects' next hop and out-of-scope links discovered mid-crawl. A run with an
   empty allow-list cannot start (`ValueError`).
2. **At-least-once, never at-most-once.** Work is retried on crash; your extractor and store must
   therefore be idempotent (the JSONL store keys on URL, the frontier dedupes by hash).
3. **Content dedupe needs real text.** Pass the article body as `item["text"]`; SimHash on boilerplate
   (nav/footer) collapses everything into one "duplicate".
4. **Threshold < bands.** Recall of the banded index is guaranteed only when `threshold < bands`
   (pigeonhole). The constructor refuses the unsafe combination rather than losing matches quietly.
5. **Audit before you need it.** `audit-verify` walks the hash chain; a single edited line breaks it.

## What this is not

This package performs **authorised collection**: public pages, within a declared scope, at a declared
rate, with an audit trail. It contains no authentication bypass, no credential handling, no exploit
code, and no evasion logic aimed at a third party's infrastructure. If a run needs an account, a
cookie, or a rate the target did not publish, that is a conversation with the target's owner — not a
flag in this CLI.

## Tests

```bash
pytest tests/ops -q     # 39 tests, no network, no browser
```

Covers: canonicalisation & dedupe, priority ordering, cross-thread exclusive leasing, lease reaping,
retry/backoff/dead-letter, scope denials (domain/pattern/depth/QPS/budget/robots), identity cooldown,
SimHash recall guarantees, redaction, audit-chain tamper detection, Prometheus output, and an
end-to-end run with a fake fetcher (including a real content duplicate and a denied admin path).
