"""Engine ↔ pacing: the delay the engine actually sleeps, burst pauses, session rotation."""

from random import Random

import pytest

import scrapling.spiders.engine as engine_module
from scrapling.engines.toolbelt.custom import Response
from scrapling.spiders.engine import CrawlerEngine
from scrapling.spiders.pacing import PacingConfig
from scrapling.spiders.presets import APT_PACING, ENGINEERING_PACING
from scrapling.spiders.request import Request
from scrapling.spiders.spider import Spider

DOMAIN = "example.com"
LONG = b"<html><body>" + b"content " * 400 + b"</body></html>"


class _Spider(Spider):
    name = "pacing"
    start_urls = [f"https://{DOMAIN}/"]

    async def parse(self, response):
        yield {"url": response.url}


def _response(url: str = f"https://{DOMAIN}/") -> Response:
    return Response(url, LONG, 200, "probe", {}, {}, {}, _log=False)


@pytest.fixture
def sleeper(monkeypatch):
    """Record sleeps instead of waiting them out."""
    slept = []

    async def fake_sleep(seconds):
        slept.append(seconds)

    monkeypatch.setattr(engine_module.anyio, "sleep", fake_sleep)
    return slept


def _engine(pacing=None, *, seed: int = 3) -> CrawlerEngine:
    spider = _Spider()
    if pacing is not None:
        spider.pacing_config = pacing
    engine = CrawlerEngine(spider, spider._session_manager)
    if engine._pacing is not None:
        engine._pacing._rng = Random(seed)
    return engine


async def _serve(engine: CrawlerEngine, path: str = "a") -> None:
    async def fetch(request):
        return _response(request.url)

    engine.session_manager.fetch = fetch
    await engine._process_request(Request(f"https://{DOMAIN}/{path}"))


class TestDelayFloor:
    @pytest.mark.asyncio
    async def test_engineering_pacing_holds_the_twenty_percent_floor(self, sleeper):
        engine = _engine(ENGINEERING_PACING)

        await _serve(engine)

        pausing = [value for value in sleeper if value >= 1.0]
        assert pausing, sleeper
        assert pausing[0] >= ENGINEERING_PACING.base_delay * (1 - ENGINEERING_PACING.jitter_ratio)

    @pytest.mark.asyncio
    async def test_engineering_pacing_never_exceeds_the_band(self, sleeper):
        engine = _engine(ENGINEERING_PACING)

        for index in range(20):
            await _serve(engine, f"p{index}")

        band = [value for value in sleeper if value >= 1.0]
        assert band
        ceiling = (
            ENGINEERING_PACING.base_delay
            * (1 + ENGINEERING_PACING.jitter_ratio)
            * max(ENGINEERING_PACING.hour_multipliers)
            * (1 + ENGINEERING_PACING.minute_jitter)
        )
        assert max(band) <= ceiling + 0.01

    @pytest.mark.asyncio
    async def test_apt_pacing_goes_below_a_second(self, sleeper):
        engine = _engine(APT_PACING)

        for index in range(10):
            await _serve(engine, f"q{index}")

        assert min(sleeper) < 1.0

    @pytest.mark.asyncio
    async def test_pacing_is_off_unless_the_spider_asks(self):
        engine = _engine()

        assert engine._pacing is None

    @pytest.mark.asyncio
    async def test_without_pacing_the_engine_waits_only_the_floor(self, sleeper):
        engine = _engine()
        engine.spider.download_delay = 0.75

        await _serve(engine)

        assert sleeper == [0.75]

    @pytest.mark.asyncio
    async def test_enabling_engineering_pacing_adds_its_delay(self, sleeper):
        engine = _engine(ENGINEERING_PACING)

        await _serve(engine)

        assert max(sleeper) >= ENGINEERING_PACING.base_delay * (1 - ENGINEERING_PACING.jitter_ratio)

    @pytest.mark.asyncio
    async def test_a_fast_pacing_never_shortens_the_download_delay(self, sleeper):
        engine = _engine(PacingConfig(base_delay=0.0, jitter_ratio=0.0, distribution="fixed", minute_jitter=0.0))
        engine.spider.download_delay = 1.5

        await _serve(engine)

        assert sleeper == [1.5]


class TestBurstOnTheWire:
    @pytest.mark.asyncio
    async def test_the_twenty_first_request_waits_for_the_pause(self, sleeper):
        engine = _engine(APT_PACING)

        for index in range(20):
            await _serve(engine, f"b{index}")
        burst_window = list(sleeper)
        await _serve(engine, "b20")

        assert max(sleeper) >= APT_PACING.burst_pause_min
        assert max(sleeper) > max(burst_window)

    @pytest.mark.asyncio
    async def test_a_disabled_burst_never_pauses(self, sleeper):
        engine = _engine(
            PacingConfig(base_delay=0.2, jitter_ratio=0.1, distribution="uniform", minute_jitter=0.0, burst_size=0)
        )

        for index in range(30):
            await _serve(engine, f"c{index}")

        assert max(sleeper) < 1.0


class TestSessionRotation:
    @pytest.mark.asyncio
    async def test_rotation_calls_reset_domain(self, sleeper, monkeypatch):
        engine = _engine(PacingConfig(base_delay=0.0, jitter_ratio=0.0, distribution="fixed",
                                      minute_jitter=0.0, rotate_session_every=2))
        resets = []

        async def fake_reset(domain):
            resets.append(domain)

        monkeypatch.setattr(engine.session_manager, "reset_domain", fake_reset)

        await _serve(engine, "r1")
        assert resets == []

        await _serve(engine, "r2")

        assert resets == [DOMAIN]

    @pytest.mark.asyncio
    async def test_rotation_pauses_before_the_new_identity_starts(self, sleeper, monkeypatch):
        engine = _engine(PacingConfig(base_delay=0.0, jitter_ratio=0.0, distribution="fixed",
                                      minute_jitter=0.0, rotate_session_every=1, rotate_pause=2.5))

        async def fake_reset(domain):
            return None

        monkeypatch.setattr(engine.session_manager, "reset_domain", fake_reset)

        await _serve(engine, "p1")

        assert 2.5 in sleeper

    @pytest.mark.asyncio
    async def test_rotation_clears_the_counter(self, sleeper, monkeypatch):
        engine = _engine(PacingConfig(base_delay=0.0, jitter_ratio=0.0, distribution="fixed",
                                      minute_jitter=0.0, rotate_session_every=2))
        resets = []

        async def fake_reset(domain):
            resets.append(domain)

        monkeypatch.setattr(engine.session_manager, "reset_domain", fake_reset)

        for index in range(3):
            await _serve(engine, f"s{index}")

        assert len(resets) == 1

    @pytest.mark.asyncio
    async def test_rotation_keeps_the_ladder_and_rebuilds_the_session(self, sleeper):
        engine = _engine(PacingConfig(base_delay=0.0, jitter_ratio=0.0, distribution="fixed",
                                      minute_jitter=0.0, rotate_session_every=2))
        sid = f"static:0:{DOMAIN}"

        await _serve(engine, "t1")
        first = engine.session_manager.get(sid)
        level_before = engine._adaptive_engine.plan(DOMAIN)[0] if engine._adaptive_engine else None

        await _serve(engine, "t2")  # rotation fires: the identity is retired
        assert sid not in engine.session_manager.session_ids

        await _serve(engine, "t3")  # next request rebuilds it
        rebuilt = engine.session_manager.get(sid)
        assert rebuilt is not first
        # A rotation refreshes identity, it does not forgive the risk the ladder learned.
        level_after = engine._adaptive_engine.plan(DOMAIN)[0] if engine._adaptive_engine else None
        assert level_after == level_before

    @pytest.mark.asyncio
    async def test_no_rotation_counting_without_pacing(self, sleeper, monkeypatch):
        engine = _engine()
        resets = []

        async def fake_reset(domain):
            resets.append(domain)

        monkeypatch.setattr(engine.session_manager, "reset_domain", fake_reset)

        for index in range(25):
            await _serve(engine, f"v{index}")

        assert engine._pacing is None
        assert resets == []

    @pytest.mark.asyncio
    async def test_no_rotation_without_the_option(self, sleeper, monkeypatch):
        engine = _engine()
        resets = []

        async def fake_reset(domain):
            resets.append(domain)

        monkeypatch.setattr(engine.session_manager, "reset_domain", fake_reset)

        for index in range(25):
            await _serve(engine, f"u{index}")

        assert resets == []
