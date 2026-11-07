"""`scrapling-ops` command line.

    scrapling-ops run    --seed https://example.com --allow example.com --max-pages 200
    scrapling-ops status --db crawl.db
    scrapling-ops export --db crawl.db --state done
    scrapling-ops audit-verify --path audit.jsonl

Every subcommand is safe with respect to the run's scope: `--allow` is mandatory,
so a mistyped command cannot silently crawl the open internet.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .adapters import generic_extractor, scrapling_fetcher
from .frontier import Frontier
from .guard import DEFAULT_DENY_PATTERNS, ScopeGuard
from .identity import Identity, IdentityPool
from .pipeline import JsonlStore, OpsRunner
from .telemetry import AuditLog, Metrics


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="scrapling-ops", description="Authorised, resumable, multi-worker crawl operations.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run", help="seed URLs and run a crawl")
    run.add_argument("--seed", action="append", required=True, help="seed URL (repeatable)")
    run.add_argument("--allow", action="append", required=True, help="allowed domain (repeatable)")
    run.add_argument("--db", default="crawl.db")
    run.add_argument("--out", default="items.jsonl")
    run.add_argument("--audit", default="audit.jsonl")
    run.add_argument("--metrics-out", default=None, help="write Prometheus snapshot here at the end")
    run.add_argument("--max-pages", type=int, default=200)
    run.add_argument("--workers", type=int, default=8)
    run.add_argument("--qps", type=float, default=1.0, help="per-domain request ceiling")
    run.add_argument("--max-depth", type=int, default=5)
    run.add_argument("--stealth", action="store_true", help="use Scrapling's stealthy fetcher")
    run.add_argument("--resume", action="store_true", help="reuse an existing frontier instead of seeding again")

    status = sub.add_parser("status", help="print frontier statistics")
    status.add_argument("--db", default="crawl.db")

    export = sub.add_parser("export", help="dump URLs by state as JSONL")
    export.add_argument("--db", default="crawl.db")
    export.add_argument("--state", default="done", choices=["pending", "leased", "done", "failed"])
    export.add_argument("--limit", type=int, default=1000)

    gc = sub.add_parser("gc", help="drop failed rows so they can be retried in a new run")
    gc.add_argument("--db", default="crawl.db")

    verify = sub.add_parser("audit-verify", help="verify the audit log hash chain")
    verify.add_argument("--path", default="audit.jsonl")

    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    if args.cmd == "status":
        frontier = Frontier(args.db)
        print(json.dumps(frontier.stats().__dict__ | {"progress": frontier.stats().progress}, indent=2))
        return 0

    if args.cmd == "export":
        frontier = Frontier(args.db)
        for record in frontier.iter_state(args.state, limit=args.limit):
            print(json.dumps(record.__dict__, ensure_ascii=False))
        return 0

    if args.cmd == "gc":
        frontier = Frontier(args.db)
        removed = frontier.gc_failed()
        print(f"removed {removed} failed rows")
        return 0

    if args.cmd == "audit-verify":
        audit = AuditLog(args.path, redact_payload=False)
        intact, count = audit.verify()
        print(json.dumps({"intact": intact, "records": count}))
        return 0 if intact else 1

    # run
    frontier = Frontier(args.db)
    metrics = Metrics()
    audit = AuditLog(args.audit)
    guard = ScopeGuard(
        args.allow,
        deny_paths=DEFAULT_DENY_PATTERNS,
        max_depth=args.max_depth,
        per_domain_qps=args.qps,
        daily_budget=None,
    )
    pool = IdentityPool([Identity(name="primary", headers={"Accept-Language": "en-US,en;q=0.9"})])
    runner = OpsRunner(
        frontier,
        guard,
        pool,
        scrapling_fetcher(stealth=args.stealth),
        generic_extractor(),
        JsonlStore(args.out),
        workers=args.workers,
        max_pages=args.max_pages,
        metrics=metrics,
        audit=audit,
    )
    if not args.resume:
        runner.seed(args.seed)
    report = runner.run()
    print(json.dumps(report.as_dict(), ensure_ascii=False, indent=2))

    if args.metrics_out:
        Path(args.metrics_out).write_text(metrics.render(), encoding="utf-8")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
