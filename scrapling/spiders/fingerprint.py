"""Whole-browser fingerprints: one profile per real browser, never a mix.

A profile pins the TLS impersonation *and* every HTTP header that carries identity, so
curl_cffi's handshake and our headers describe the same browser. Mixing them is worse
than a plain spoof: a Chrome JA3 with a Safari `Accept` is a fingerprint of its own.

`header_order` is informational on the static path (curl_cffi owns wire order) and is
there for the browser rung, where a Playwright `init_script` can replay it.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from random import choice as random_choice
from threading import Lock
from typing import Dict, Iterable, List, Optional, Tuple
from zlib import crc32

__all__ = [
    "APT_FINGERPRINTS",
    "BrowserFingerprint",
    "DEFAULT_FINGERPRINTS",
    "FingerprintPool",
]


@dataclass(frozen=True)
class BrowserFingerprint:
    name: str
    impersonate: str
    user_agent: str
    accept: str
    accept_language: str
    accept_encoding: str
    sec_ch_ua: Optional[str] = None  # 仅 Chrome/Edge 有
    sec_ch_ua_mobile: Optional[str] = None
    sec_ch_ua_platform: Optional[str] = None
    header_order: Tuple[str, ...] = ()  # 头顺序，curl_cffi 不直接支持，但留作 Playwright init_script 用
    extra_headers: Dict[str, str] = field(default_factory=dict)

    def to_headers(self) -> dict:
        """返回该 profile 对应的完整 header dict（不含 curl_cffi 自己加的那几个）。"""
        headers: Dict[str, str] = {
            "User-Agent": self.user_agent,
            "Accept": self.accept,
            "Accept-Language": self.accept_language,
            "Accept-Encoding": self.accept_encoding,
        }
        if self.sec_ch_ua is not None:
            headers["sec-ch-ua"] = self.sec_ch_ua
            if self.sec_ch_ua_mobile is not None:
                headers["sec-ch-ua-mobile"] = self.sec_ch_ua_mobile
            if self.sec_ch_ua_platform is not None:
                headers["sec-ch-ua-platform"] = self.sec_ch_ua_platform
        headers.update(self.extra_headers)

        ordered = {name: headers[name] for name in self.header_order if name in headers}
        ordered.update({name: value for name, value in headers.items() if name not in ordered})
        return ordered

    def apply_to_session_kwargs(self, kwargs: dict) -> dict:
        """把 profile 注入到 request._session_kwargs：设置 headers、impersonate。"""
        kwargs["impersonate"] = self.impersonate
        kwargs["headers"] = self.to_headers()
        return kwargs

    def with_name(self, name: str) -> "BrowserFingerprint":
        return replace(self, name=name)


_CHROME_ACCEPT = (
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,"
    "*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
)
_CHROME_ORDER = (
    "Host",
    "Connection",
    "sec-ch-ua",
    "sec-ch-ua-mobile",
    "sec-ch-ua-platform",
    "Upgrade-Insecure-Requests",
    "User-Agent",
    "Accept",
    "Sec-Fetch-Site",
    "Sec-Fetch-Mode",
    "Sec-Fetch-User",
    "Sec-Fetch-Dest",
    "Referer",
    "Accept-Encoding",
    "Accept-Language",
)
# Safari puts Accept-Encoding before Accept and never sends client hints.
_SAFARI_ORDER = ("Host", "Accept-Encoding", "Connection", "Accept", "User-Agent", "Accept-Language", "Referer")
_FIREFOX_ORDER = (
    "Host",
    "User-Agent",
    "Accept",
    "Accept-Language",
    "Accept-Encoding",
    "Connection",
    "Upgrade-Insecure-Requests",
    "Sec-Fetch-Dest",
    "Sec-Fetch-Mode",
    "Sec-Fetch-Site",
    "Sec-Fetch-User",
    "Referer",
)


CHROME_120 = BrowserFingerprint(
    name="chrome120",
    impersonate="chrome120",
    user_agent=(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    accept=_CHROME_ACCEPT,
    accept_language="zh-CN,zh;q=0.9,en;q=0.8",
    accept_encoding="gzip, deflate, br, zstd",
    sec_ch_ua='"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    sec_ch_ua_mobile="?0",
    sec_ch_ua_platform='"Windows"',
    header_order=_CHROME_ORDER,
)

CHROME_131 = BrowserFingerprint(
    name="chrome131",
    impersonate="chrome131",
    user_agent=(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    accept=_CHROME_ACCEPT,
    accept_language="zh-CN,zh;q=0.9,en;q=0.8",
    accept_encoding="gzip, deflate, br, zstd",
    sec_ch_ua='"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    sec_ch_ua_mobile="?0",
    sec_ch_ua_platform='"Windows"',
    header_order=_CHROME_ORDER,
)

SAFARI_17_2_IOS = BrowserFingerprint(
    name="safari17_2_ios",
    impersonate="safari17_2_ios",
    user_agent=(
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1"
    ),
    accept="text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    accept_language="zh-CN,zh-Hans;q=0.9",
    accept_encoding="gzip, deflate, br",
    header_order=_SAFARI_ORDER,
)

SAFARI_17_2_MACOS = SAFARI_17_2_IOS.with_name("safari17_2_macos")
SAFARI_17_2_MACOS = replace(
    SAFARI_17_2_MACOS,
    user_agent=(
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Version/17.2 Safari/605.1.15"
    ),
)

FIREFOX_133 = BrowserFingerprint(
    name="firefox133",
    impersonate="firefox133",
    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    accept="text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    accept_language="zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
    accept_encoding="gzip, deflate, br, zstd",
    header_order=_FIREFOX_ORDER,
)

FIREFOX_133_MAC = replace(
    FIREFOX_133.with_name("firefox133_mac"),
    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:133.0) Gecko/20100101 Firefox/133.0",
)

EDGE_131 = BrowserFingerprint(
    name="edge131",
    # curl_cffi has no edge131; edge101 carries the closest Edge TLS stack, so the header
    # set below is what has to make the two agree.
    impersonate="edge101",
    user_agent=(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
    ),
    accept=_CHROME_ACCEPT,
    accept_language="zh-CN,zh;q=0.9,en;q=0.8",
    accept_encoding="gzip, deflate, br, zstd",
    sec_ch_ua='"Microsoft Edge";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    sec_ch_ua_mobile="?0",
    sec_ch_ua_platform='"Windows"',
    header_order=_CHROME_ORDER,
)

CHROME_120_MAC = replace(
    CHROME_120.with_name("chrome120_mac"),
    user_agent=(
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    sec_ch_ua_platform='"macOS"',
)

DEFAULT_FINGERPRINTS: Tuple[BrowserFingerprint, ...] = (
    CHROME_120,
    CHROME_131,
    SAFARI_17_2_IOS,
    FIREFOX_133,
    EDGE_131,
)

APT_FINGERPRINTS: Tuple[BrowserFingerprint, ...] = DEFAULT_FINGERPRINTS + (
    CHROME_120_MAC,
    SAFARI_17_2_MACOS,
    FIREFOX_133_MAC,
)


class FingerprintPool:
    """Name-addressable set of profiles with random / round-robin / per-domain picking."""

    def __init__(self, fingerprints: Optional[Iterable[BrowserFingerprint]] = None) -> None:
        items = tuple(fingerprints) if fingerprints is not None else DEFAULT_FINGERPRINTS
        if not items:
            raise ValueError("fingerprint pool needs at least one profile")
        by_name: Dict[str, BrowserFingerprint] = {}
        for item in items:
            if item.name in by_name:
                raise ValueError(f"duplicate fingerprint name: {item.name}")
            by_name[item.name] = item
        self._items = items
        self._by_name = by_name
        self._round_robin = 0
        self._lock = Lock()

    def __len__(self) -> int:
        return len(self._items)

    def __iter__(self):
        return iter(self._items)

    def all_names(self) -> List[str]:
        return [item.name for item in self._items]

    def get(self, name: str) -> BrowserFingerprint:
        try:
            return self._by_name[name]
        except KeyError:
            raise KeyError(f"Fingerprint '{name}' not found. Available: {', '.join(self.all_names())}") from None

    def pick(self, strategy: str = "random") -> BrowserFingerprint:
        """strategy: random / round_robin / sticky（同域名固定一个，见 pick_for_domain）。"""
        if strategy == "round_robin":
            with self._lock:
                item = self._items[self._round_robin % len(self._items)]
                self._round_robin += 1
            return item
        # "sticky" without a domain has nothing to be sticky to; it degrades to random.
        return random_choice(self._items)

    def pick_for_domain(self, domain: str, strategy: str = "sticky", *, salt: int = 0) -> BrowserFingerprint:
        """按域名稳定选一个，保证同一域名每次重建 session 都用同一个 profile.

        `salt` lets the caller rotate the choice deliberately — a session reset passes a
        new one, so a burnt fingerprint is not handed straight back to the same host.
        """
        if strategy != "sticky":
            return self.pick(strategy)
        index = (crc32((domain or "__default__").encode()) + salt) % len(self._items)
        return self._items[index]
