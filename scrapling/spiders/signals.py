"""Anti-bot signal recognition: one response in, one verdict out.

Pure functions only — no IO, no clock, no state. The precedence below is the whole
design: a Cloudflare interstitial contains the word "captcha" in its inline JS, and
a login redirect contains "signin" in both the Location header *and* the body, so
the checks run strongest-evidence-first and return on the first hit.

Precedence: CHALLENGE > CAPTCHA > LOGIN_WALL > RATELIMIT > FORBIDDEN > EMPTY > NONE.

Confidences are deliberately coarse: they feed `AdaptiveEngine`, which multiplies
them by a per-signal risk budget. Four decimals of precision there would be noise.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto

from scrapling.core._types import Any, Dict, Optional

__all__ = ["BlockSignal", "SignalResult", "detect_signal"]

CHALLENGE_HEADERS = ("cf-mitigated", "cf-chl-bypass")

CHALLENGE_MARKERS = (
    "__cf_chl_opt",
    "_cf_chl_opt",
    "cf-challenge",
    "challenge-platform",
    "just a moment",
    "checking your browser",
    "ddos-guard",
    "sgcaptcha",
)

CAPTCHA_MARKERS = (
    "captcha",
    "recaptcha",
    "hcaptcha",
    "g-recaptcha",
    "cf-turnstile",
    "请输入验证码",
    "请完成验证",
)

CAPTCHA_PATHS = ("/captcha", "/verify", "/challenge")

LOGIN_PATHS = ("/login", "/signin", "/auth", "/sso")

LOGIN_MARKERS = ("sign in to continue", "登录后查看", "请先登录")

RATELIMIT_MARKERS = ("rate limit", "too many requests", "请求过于频繁")

FORBIDDEN_STATUSES = frozenset({401, 403, 407, 444})

REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})


class BlockSignal(Enum):
    """What a single response looks like from the anti-bot side."""

    CAPTCHA = auto()  # 验证码页
    LOGIN_WALL = auto()  # 登录墙
    RATELIMIT = auto()  # 限流页
    FORBIDDEN = auto()  # 403 / 401 硬封
    CHALLENGE = auto()  # JS challenge / 5s shield
    EMPTY = auto()  # 200 但内容过短
    NONE = auto()  # 无信号


@dataclass(frozen=True)
class SignalResult:
    """A verdict plus the evidence that produced it."""

    signal: BlockSignal
    confidence: float
    reason: str


def _text(body: Any) -> str:
    """Decode whatever the fetch layer handed over, never raising on bad bytes."""
    if isinstance(body, str):
        return body
    if isinstance(body, (bytes, bytearray)):
        return bytes(body).decode("utf-8", "replace")
    return "" if body is None else str(body)


def _headers(response: Any) -> Dict[str, str]:
    """Lower-cased header map; `Response.headers` may be a plain dict or a case-insensitive view."""
    raw = getattr(response, "headers", None) or {}
    try:
        items = raw.items()
    except AttributeError:
        return {}
    return {str(key).lower(): str(value) for key, value in items}


def _url(response: Any) -> str:
    """Final URL, falling back to the request URL for hand-built responses."""
    for attribute in ("url", "request_url"):
        value = getattr(response, attribute, None)
        if isinstance(value, str) and value:
            return value
    request = getattr(response, "request", None)
    value = getattr(request, "url", None)
    return value if isinstance(value, str) else ""


def _first_marker(haystack: str, markers: tuple[str, ...]) -> Optional[str]:
    return next((marker for marker in markers if marker in haystack), None)


def detect_signal(response: Any, min_body_bytes: int = 512) -> SignalResult:
    """识别单个响应的反爬信号。纯函数，可单测。

    :param response: anything exposing `status`, `body`, `headers` and (optionally) `url`.
    :param min_body_bytes: below this, a 200 reads as EMPTY — the classic soft block.
    """
    status = int(getattr(response, "status", 0) or 0)
    body = _text(getattr(response, "body", b""))
    lowered = body.lower()
    headers = _headers(response)
    url = _url(response).lower()

    hit = next((name for name in CHALLENGE_HEADERS if name in headers), None)
    if hit:
        return SignalResult(BlockSignal.CHALLENGE, 0.9, f"header {hit}")

    hit = _first_marker(lowered, CHALLENGE_MARKERS)
    if hit:
        return SignalResult(BlockSignal.CHALLENGE, 0.9, f"body contains {hit!r}")

    hit = _first_marker(lowered, CAPTCHA_MARKERS)
    if hit:
        return SignalResult(BlockSignal.CAPTCHA, 0.85, f"body contains {hit!r}")

    hit = _first_marker(url, CAPTCHA_PATHS)
    if hit:
        return SignalResult(BlockSignal.CAPTCHA, 0.85, f"url contains {hit}")

    hit = _first_marker(url, LOGIN_PATHS)
    if hit:
        return SignalResult(BlockSignal.LOGIN_WALL, 0.8, f"url contains {hit}")

    hit = _first_marker(lowered, LOGIN_MARKERS)
    if hit:
        return SignalResult(BlockSignal.LOGIN_WALL, 0.8, f"body contains {hit!r}")

    location = headers.get("location", "").lower()
    if status in REDIRECT_STATUSES and _first_marker(location, LOGIN_PATHS):
        return SignalResult(BlockSignal.LOGIN_WALL, 0.8, f"{status} redirect to {headers['location']!r}")

    if status == 429:
        return SignalResult(BlockSignal.RATELIMIT, 0.95, "status 429")

    hit = next((name for name in ("retry-after",) if name in headers), None)
    if hit:
        return SignalResult(BlockSignal.RATELIMIT, 0.95, f"header {hit}")

    if headers.get("x-ratelimit-remaining", "").strip() == "0":
        return SignalResult(BlockSignal.RATELIMIT, 0.95, "header x-ratelimit-remaining: 0")

    hit = _first_marker(lowered, RATELIMIT_MARKERS)
    if hit:
        return SignalResult(BlockSignal.RATELIMIT, 0.95, f"body contains {hit!r}")

    if status in FORBIDDEN_STATUSES:
        return SignalResult(BlockSignal.FORBIDDEN, 0.9, f"status {status}")

    if status == 200 and len(body.encode("utf-8")) < min_body_bytes:
        return SignalResult(BlockSignal.EMPTY, 0.7, f"200 with {len(body.encode('utf-8'))}B body")

    return SignalResult(BlockSignal.NONE, 0.0, "no anti-bot signal")
