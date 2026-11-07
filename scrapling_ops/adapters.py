"""Adapters between `scrapling_ops` and Scrapling's fetchers/parser.

Both are lazily imported: the ops layer is usable (and testable) without Scrapling
installed, which keeps CI fast and lets you plug a different fetcher in.

The fetch adapter is deliberately conservative — if Scrapling's keyword arguments
change between versions, we degrade to the smallest common call instead of
crashing the run.
"""

from __future__ import annotations

import re
import time
from typing import Callable, Optional

from .pipeline import FetchResult

DEFAULT_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8",
    "Cache-Control": "no-cache",
}

_TAG_RE = re.compile(r"<(script|style|noscript)[^>]*>.*?</\1>", re.S | re.I)
_ANY_TAG_RE = re.compile(r"<[^>]+>")
_HREF_RE = re.compile(r"""href=["']([^"'#]+)["']""", re.I)
_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.S | re.I)


def scrapling_fetcher(
    *,
    stealth: bool = False,
    timeout: float = 30.0,
    impersonate: Optional[str] = "chrome",
    extra_headers: Optional[dict] = None,
) -> Callable:
    """Return a `Fetcher` backed by Scrapling (static or stealthy engine)."""

    def _fetch(url: str, identity) -> FetchResult:
        started = time.time()
        headers = dict(DEFAULT_HEADERS)
        headers.update(identity.headers or {})
        headers.update(extra_headers or {})
        try:
            from scrapling.fetchers import Fetcher, StealthyFetcher  # noqa: PLC0415

            if stealth:
                page = StealthyFetcher.fetch(url, headless=True, timeout=int(timeout * 1000), headers=headers)
            else:
                kwargs = {"headers": headers, "stealthy_headers": True, "timeout": timeout}
                if impersonate:
                    kwargs["impersonate"] = impersonate
                try:
                    page = Fetcher.get(url, **kwargs)
                except TypeError:  # older/newer signature
                    page = Fetcher.get(url, headers=headers, timeout=timeout)
            status = int(getattr(page, "status", 200) or 200)
            body = getattr(page, "html_content", None) or str(page)
            return FetchResult(ok=200 <= status < 400, status=status, body=body, url=getattr(page, "url", url), elapsed=time.time() - started)
        except Exception as exc:  # network error, missing extra, API drift
            return FetchResult(ok=False, status=0, body="", url=url, elapsed=time.time() - started, error=f"{type(exc).__name__}: {exc}")

    return _fetch


def generic_extractor(
    *,
    text_selector: Optional[str] = None,
    link_selector: str = "a[href]",
    max_text: int = 200_000,
) -> Callable:
    """Extract `{title, text}` + same-page links. Uses Scrapling's parser when present."""

    def _extract(body: str, url: str):
        title = ""
        try:
            from scrapling.parser import Selector  # noqa: PLC0415

            page = Selector(body)
            for expr in ("title::text", "title", "h1::text"):
                try:
                    value = page.css(expr)
                except Exception:
                    value = []
                if value:
                    title = (value[0].text if hasattr(value[0], "text") else str(value[0])).strip()
                    if title:
                        break
            node = page.css(text_selector)[0] if text_selector else None
            text = (node.get_all_text(strip=True) if node is not None else page.get_all_text(strip=True)) or ""
            links = [a.attrib.get("href", "") for a in page.css(link_selector)]
        except Exception:
            match = _TITLE_RE.search(body)
            title = match.group(1).strip() if match else ""
            cleaned = _ANY_TAG_RE.sub(" ", _TAG_RE.sub(" ", body))
            text = re.sub(r"\s+", " ", cleaned).strip()
            links = _HREF_RE.findall(body)
        # Relative hrefs are the norm; resolve them here so the runner only ever
        # sees absolute URLs (the guard can then judge the real destination host).
        absolute = absolute_links([link for link in links if link and not link.startswith(("#", "javascript:", "mailto:"))], url)
        return {"url": url, "title": title.strip(), "text": text[:max_text]}, absolute

    return _extract


def absolute_links(links: list[str], base: str) -> list[str]:
    from urllib.parse import urljoin

    return [urljoin(base, link) for link in links]
