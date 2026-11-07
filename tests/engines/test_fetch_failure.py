"""Fetch failure snapshots: recorded on every failed attempt, capped, summarised."""

import pytest

from scrapling.engines._browsers._stealth import AsyncStealthySession, FetchFailure

URL = "https://example.com/page"


class _FailingPage:
    """Minimal Playwright page whose navigation always times out."""

    def __init__(self) -> None:
        self.closed = False
        self.listeners = []

    def set_default_navigation_timeout(self, value):
        pass

    def set_default_timeout(self, value):
        pass

    async def set_extra_http_headers(self, headers):
        pass

    async def unroute_all(self, behavior=None):
        pass

    async def route(self, pattern, handler):
        raise AssertionError("no interception expected here")

    def on(self, event, handler):
        self.listeners.append((event, handler))

    def remove_listener(self, event, handler):
        pass

    async def goto(self, url, referer=None):
        raise TimeoutError("page.goto: Timeout 30000ms exceeded")

    async def wait_for_timeout(self, ms):
        pass

    def is_closed(self):
        return self.closed

    async def close(self):
        self.closed = True


class _Context:
    def __init__(self) -> None:
        self.pages = []

    async def new_page(self):
        page = _FailingPage()
        self.pages.append(page)
        return page


def _session(retries: int = 3) -> AsyncStealthySession:
    session = AsyncStealthySession(max_pages=1, retries=retries, retry_delay=0)
    session.context = _Context()
    session._is_alive = True
    return session


@pytest.mark.asyncio
async def test_every_failed_attempt_leaves_a_record():
    session = _session(retries=3)

    with pytest.raises(TimeoutError):
        await session.fetch(URL)

    assert len(session._failures) == 3
    assert [f.attempt for f in session._failures] == [1, 2, 3]
    assert all(f.exc_type == "TimeoutError" for f in session._failures)


@pytest.mark.asyncio
async def test_recent_failures_returns_the_tail():
    session = _session(retries=3)
    with pytest.raises(TimeoutError):
        await session.fetch(URL)

    tail = session.recent_failures(limit=2)

    assert len(tail) == 2
    assert [f.attempt for f in tail] == [2, 3]
    assert tail is not session._failures


@pytest.mark.asyncio
async def test_failure_summary_groups_by_type():
    session = _session(retries=2)
    with pytest.raises(TimeoutError):
        await session.fetch(URL)

    summary = session.failure_summary()

    assert summary == {"total": 2, "by_type": {"TimeoutError": 2}}
    assert session.recent_failures()[-1].exc_msg


@pytest.mark.asyncio
async def test_a_record_carries_everything_needed_to_debug():
    session = _session(retries=1)
    with pytest.raises(TimeoutError):
        await session.fetch(URL)

    failure = session.recent_failures(1)[0]

    assert isinstance(failure, FetchFailure)
    assert failure.url == URL
    assert failure.exc_type == "TimeoutError"
    assert "Timeout" in failure.exc_msg
    assert failure.attempt == 1
    assert failure.elapsed_ms >= 0
    assert failure.level == "browser"
    assert failure.timestamp > 0


@pytest.mark.asyncio
async def test_the_history_is_capped_at_one_hundred():
    session = _session(retries=10)
    for _ in range(11):
        with pytest.raises(TimeoutError):
            await session.fetch(URL)

    assert len(session._failures) == 100
    # 110 attempts happened; the ten oldest were dropped, so the tail is the last fetch.
    assert [f.attempt for f in session._failures[-10:]] == list(range(1, 11))


@pytest.mark.asyncio
async def test_the_longest_message_is_truncated():
    session = _session(retries=1)
    original = _FailingPage.goto

    async def verbose_goto(self, url, referer=None):
        raise RuntimeError("x" * 500)

    _FailingPage.goto = verbose_goto
    try:
        with pytest.raises(RuntimeError):
            await session.fetch(URL)
    finally:
        _FailingPage.goto = original

    assert len(session.recent_failures(1)[0].exc_msg) == 200
