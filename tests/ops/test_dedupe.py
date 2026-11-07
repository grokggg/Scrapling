from __future__ import annotations

from scrapling_ops.dedupe import NearDupIndex, content_hash, hamming64, simhash64

BASE = (
    "The crawler fetches pages through a pool of identities, checks scope before every request, "
    "and records each decision in an append-only audit log so the run can be reviewed later."
)


def test_identical_text_is_zero_distance():
    a, b = simhash64(BASE), simhash64(BASE)
    assert a == b and hamming64(a, b) == 0


def test_near_duplicate_is_caught():
    edited = BASE.replace("fetches pages", "downloads pages")
    distance = hamming64(simhash64(BASE), simhash64(edited))
    assert 0 < distance <= 6
    index = NearDupIndex(threshold=6)
    assert index.add("a", simhash64(BASE)) is None
    hit = index.add("b", simhash64(edited))
    assert hit is not None and hit.key == "a"


def test_unrelated_text_is_not_a_duplicate():
    other = "Quarterly revenue rose while operating margin contracted, and management guided lower for the year."
    index = NearDupIndex(threshold=4)
    index.add("a", simhash64(BASE))
    assert index.add("b", simhash64(other)) is None


def test_chinese_text_uses_char_grams():
    zh = "这是一个用于采集公开网页内容的爬虫框架，所有请求都在授权范围内进行。"

    def fake():
        nonlocal zh
        zh += "它会记录每一次请求以便审计。"

    first = simhash64(zh)
    fake()
    second = simhash64(zh)
    assert first and second
    assert hamming64(first, second) <= 16


def test_content_hash_ignores_whitespace_case():
    assert content_hash("Hello   World") == content_hash("hello world\n")


def test_empty_text_has_no_signal():
    assert simhash64("") == 0
    assert NearDupIndex().add("empty", 0) is None


def test_index_reports_stats():
    index = NearDupIndex(threshold=6)
    index.add("a", simhash64(BASE))
    stats = index.stats()
    assert stats["items"] == 1 and stats["bands"] == 8


def test_threshold_must_be_below_bands():
    import pytest

    with pytest.raises(ValueError):
        NearDupIndex(threshold=8, bands=8)
