"""Render pool: `max_pages` slots must allow that many tabs at once, and no more."""

import asyncio

import pytest

from scrapling.engines._browsers._stealth import AsyncStealthySession


class _FakePage:
    """The slice of the Playwright page API that `_get_page` / `_page_generator` touch."""

    def __init__(self) -> None:
        self.closed = False
        self.nav_timeout = None
        self.default_timeout = None
        self.headers = None

    def set_default_navigation_timeout(self, value):
        self.nav_timeout = value

    def set_default_timeout(self, value):
        self.default_timeout = value

    async def set_extra_http_headers(self, headers):
        self.headers = headers

    async def unroute_all(self, behavior=None):
        return None

    async def route(self, pattern, handler):
        raise AssertionError("no interception expected in these tests")

    def is_closed(self) -> bool:
        return self.closed

    async def close(self):
        self.closed = True


class _FakeContext:
    def __init__(self) -> None:
        self.pages = []

    async def new_page(self):
        page = _FakePage()
        self.pages.append(page)
        return page


def _session(max_pages: int) -> AsyncStealthySession:
    session = AsyncStealthySession(max_pages=max_pages)
    session.context = _FakeContext()
    return session


async def _wait_until(predicate, timeout: float = 2.0) -> bool:
    for _ in range(int(timeout / 0.01)):
        if predicate():
            return True
        await asyncio.sleep(0.01)
    return False


@pytest.mark.asyncio
async def test_one_slot_serialises_two_holders():
    session = _session(1)
    entered = []
    release = asyncio.Event()

    async def holder(name):
        async with session._page_generator(1000, None, False):
            entered.append(name)
            await release.wait()

    first = asyncio.create_task(holder("first"))
    assert await _wait_until(lambda: entered == ["first"])

    second = asyncio.create_task(holder("second"))
    await asyncio.sleep(0.15)
    assert entered == ["first"], "the second holder must wait for the only slot"

    release.set()
    await asyncio.gather(first, second)
    assert entered == ["first", "second"]


@pytest.mark.asyncio
async def test_four_slots_hold_four_pages_at_once():
    session = _session(4)
    entered = set()
    all_in = asyncio.Event()
    release = asyncio.Event()

    async def holder(name):
        async with session._page_generator(1000, None, False):
            entered.add(name)
            if len(entered) == 4:
                all_in.set()
            await release.wait()

    tasks = [asyncio.create_task(holder(f"h{i}")) for i in range(4)]

    await asyncio.wait_for(all_in.wait(), timeout=2.0)

    assert len(entered) == 4
    assert session.page_pool.pages_count == 4
    assert session.page_pool.busy_count == 4

    release.set()
    await asyncio.gather(*tasks)


@pytest.mark.asyncio
async def test_the_fifth_holder_waits_for_a_free_slot():
    session = _session(4)
    entered = []
    four_in = asyncio.Event()
    gate = asyncio.Event()

    async def holder(name):
        async with session._page_generator(1000, None, False):
            entered.append(name)
            if len(entered) == 4:
                four_in.set()
            await gate.wait()

    tasks = [asyncio.create_task(holder(f"h{i}")) for i in range(4)]
    await asyncio.wait_for(four_in.wait(), timeout=2.0)

    fifth = asyncio.create_task(holder("fifth"))
    await asyncio.sleep(0.15)
    assert len(entered) == 4, "the fifth holder cannot enter while all four slots are held"

    gate.set()
    await asyncio.gather(*tasks, fifth)
    assert "fifth" in entered


@pytest.mark.asyncio
async def test_a_slot_that_never_frees_raises_timeout():
    session = _session(1)
    session._max_wait_for_page = 0.5  # keep the test fast; the production value is 60s
    hold = asyncio.Event()
    entered = asyncio.Event()

    async def stuck():
        async with session._page_generator(1000, None, False):
            entered.set()
            await hold.wait()

    holder = asyncio.create_task(stuck())
    await asyncio.wait_for(entered.wait(), timeout=2.0)

    with pytest.raises(TimeoutError) as excinfo:
        async with session._page_generator(1000, None, False):
            pass

    assert "No page slot became free within the 0.5s timeout period" in str(excinfo.value)

    hold.set()
    await holder


@pytest.mark.asyncio
async def test_the_slot_is_returned_when_the_page_errors():
    session = _session(1)

    async with session._page_generator(1000, None, False) as page_info:
        page_info.mark_error()
        page_info.page.closed = True

    # A failed page is dropped, and its slot must come back for the next request.
    assert session.page_pool.pages_count == 0
    assert session._page_sem._value == 1

    async with session._page_generator(1000, None, False):
        pass

    assert session.page_pool.pages_count == 1


@pytest.mark.asyncio
async def test_a_failed_page_creation_returns_the_slot():
    session = _session(1)

    class _BoomContext:
        async def new_page(self):
            raise RuntimeError("browser went away")

    session.context = _BoomContext()

    for _ in range(3):
        with pytest.raises(RuntimeError, match="browser went away"):
            await session._get_page(1000, None, False)

    # Three failures in a row would have drained a leaking semaphore to zero.
    assert session._page_sem._value == 1
