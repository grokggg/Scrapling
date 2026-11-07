"""P1 对照实证：原版 blocked-retry 与新版 RetryPolicy 对 429/5xx 的恢复率。

跑法（在仓库根目录）::

    python3 scripts/p1_bench.py                  # 100 URL/组，退避按 1/25 缩放，约 3 秒
    python3 scripts/p1_bench.py --base-delay 0.5 # 生产退避 0.5/1/2/4s，耗时数分钟
    python3 scripts/p1_bench.py --urls 300 --seed 7 --status 429

做法:
    werkzeug 在本机随机端口起一个 “前 N 次返回 5xx/429，之后返回 200” 的端点；
    每组 100 个 URL，每个 URL 的失败次数在 1..4 之间随机（两组用同一份随机序列）。

    legacy = 原版语义：BLOCKED_CODES 含 9 个状态码，max_blocked_retries=3，
             重入队不等待（原 engine 的 blocked 分支没有退避）。
    modern = 新版语义：BLOCKED_CODES 只剩 4 个硬封禁，5xx/429 交给
             RetryPolicy(max_attempts=5) + make_retry + 真实指数退避。

    退避默认缩放 25 倍只为让脚本几秒跑完；重试决策表、预算、计数全是生产实现。
"""

import argparse
import asyncio
import logging
import random
import statistics
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from werkzeug.serving import make_server

from scrapling.spiders.request import Request
from scrapling.spiders.retry import RetryEngine, RetryPolicy, attempt_of, make_retry

OLD_BLOCKED_CODES = {401, 403, 407, 429, 444, 500, 502, 503, 504}
OLD_MAX_BLOCKED_RETRIES = 3

_hits: dict[str, int] = defaultdict(int)


def _app(environ, start_response, status: int):
    """GET /flaky/<key>/<fail> -> <fail> 次 status，之后 200。"""
    _, _, key, fail = environ["PATH_INFO"].split("/", 3)
    served = _hits[key]
    _hits[key] = served + 1
    status = status if served < int(fail) else 200
    body = b'{"ok": true}' if status == 200 else b'{"error": "transient"}'
    start_response(
        f"{status} {'OK' if status == 200 else 'ERROR'}",
        [("Content-Type", "application/json"), ("Content-Length", str(len(body)))],
    )
    return [body]


def start_server(status: int):
    logging.getLogger("werkzeug").setLevel(logging.ERROR)  # access log would drown the table
    server = make_server(
        "127.0.0.1", 0, lambda environ, start_response: _app(environ, start_response, status), threaded=True
    )
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, f"http://127.0.0.1:{server.server_port}"


def _get(url: str) -> int:
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code


async def legacy_run(url: str) -> tuple[bool, int, int]:
    """原版 engine 的 blocked 分支：命中 9 码就重入队，最多 max_blocked_retries 次，无退避。"""
    attempts = 0
    while True:
        status = await asyncio.to_thread(_get, url)
        if status in OLD_BLOCKED_CODES and attempts < OLD_MAX_BLOCKED_RETRIES:
            attempts += 1
            continue
        return status == 200, attempts + 1, attempts


async def modern_run(url: str, policy: RetryPolicy) -> tuple[bool, int, int]:
    """新版 engine：RetryEngine 决策 + make_retry + 退避 + _bump_retry。"""
    engine = RetryEngine(policy)
    request = Request(url, sid="default")
    attempts = 0
    while True:
        status = await asyncio.to_thread(_get, request.url)
        response = _status_only(status)
        decision, delay = engine.decide(request, response=response)
        if decision.name == "GIVE_UP":
            return status == 200, attempts + 1, attempt_of(request)
        if delay > 0:
            await asyncio.sleep(delay)
        request = make_retry(request, decision)
        request._retry_count = getattr(request, "_retry_count", 0) + 1
        attempts += 1


class _status_only:
    """只有状态码的最小 response，够 decide() 用。"""

    def __init__(self, status: int):
        self.status = status
        self.headers: dict[str, str] = {}


async def measure(runner, urls: list[str], concurrency: int) -> dict:
    semaphore = asyncio.Semaphore(concurrency)
    per_url: list[tuple[bool, int, int, float]] = []

    async def one(url: str):
        async with semaphore:
            started = time.perf_counter()
            ok, total, retries = await runner(url)
            per_url.append((ok, total, retries, (time.perf_counter() - started) * 1000))

    await asyncio.gather(*(one(url) for url in urls))
    return {
        "urls": len(per_url),
        "success": sum(1 for ok, *_ in per_url if ok),
        "requests": sum(total for _, total, *_ in per_url),
        "max_retries": max(retries for *_, retries, _ in per_url),
        "avg_retries": statistics.mean(retries for *_, retries, _ in per_url),
        "avg_ms": statistics.mean(elapsed for *_, elapsed in per_url),
    }


def row(name: str, result: dict) -> str:
    rate = 100.0 * result["success"] / result["urls"]
    return (
        f"| {name:<8} | {result['success']:>3}/{result['urls']:<3} | {rate:>6.1f}% "
        f"| {result['requests']:>5} | {result['avg_retries']:>4.2f} | {result['max_retries']:>4} "
        f"| {result['avg_ms']:>8.1f} |"
    )


async def main() -> int:
    parser = argparse.ArgumentParser(description="P1 retry-recovery benchmark")
    parser.add_argument("--urls", type=int, default=100)
    parser.add_argument("--base-delay", type=float, default=0.02, help="生产值 0.5；0.02 = 缩放 25 倍")
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--seed", type=int, default=20261107)
    parser.add_argument("--status", type=int, default=0, help="0 = 500 与 429 都跑")
    args = parser.parse_args()

    rng = random.Random(args.seed)
    failures = [rng.randint(1, 4) for _ in range(args.urls)]
    policy = RetryPolicy(max_attempts=5, base_delay=args.base_delay, jitter=0.0)
    statuses = [args.status] if args.status else [500, 429]

    for status in statuses:
        server, base = start_server(status)
        _hits.clear()
        try:
            legacy_urls = [f"{base}/flaky/legacy-{status}-{i}/{fail}" for i, fail in enumerate(failures)]
            modern_urls = [f"{base}/flaky/modern-{status}-{i}/{fail}" for i, fail in enumerate(failures)]
            legacy = await measure(legacy_run, legacy_urls, args.concurrency)
            modern = await measure(lambda url: modern_run(url, policy), modern_urls, args.concurrency)
        finally:
            server.shutdown()

        print(f"\n### HTTP {status}（每个 URL 前 1..4 次失败，n={args.urls}，base_delay={args.base_delay}s）\n")
        print("| 组别     | 成功     | 成功率  | 总请求 | 平均重试 | 最大重试 | 平均耗时(ms) |")
        print("| -------- | -------- | ------- | ------ | -------- | -------- | ------------ |")
        print(row("legacy", legacy))
        print(row("modern", modern))
        gain = 100.0 * modern["success"] / modern["urls"] - 100.0 * legacy["success"] / legacy["urls"]
        print(
            f"\n恢复率 {100.0 * legacy['success'] / legacy['urls']:.1f}% → "
            f"{100.0 * modern['success'] / modern['urls']:.1f}%（{gain:+.1f} 个百分点）；"
            f"多花 {modern['requests'] - legacy['requests']} 次请求换回 "
            f"{modern['success'] - legacy['success']} 个 URL。"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
