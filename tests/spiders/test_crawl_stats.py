"""The five storage/incremental counters are real fields now, not custom_stats entries."""

from scrapling.spiders.result import CrawlStats

FIELDS = ("incremental_hits", "incremental_misses", "stored_new", "stored_updated", "dedup_skipped")


def test_the_five_new_fields_default_to_zero():
    stats = CrawlStats()

    assert {name: getattr(stats, name) for name in FIELDS} == dict.fromkeys(FIELDS, 0)


def test_each_field_increments_on_its_own():
    stats = CrawlStats()

    for name in FIELDS:
        setattr(stats, name, getattr(stats, name) + 1)
        assert getattr(stats, name) == 1

    stats.stored_new += 4
    assert stats.stored_new == 5
    assert stats.stored_updated == 1


def test_to_dict_carries_the_new_keys():
    stats = CrawlStats()
    stats.incremental_hits = 3
    stats.incremental_misses = 2
    stats.stored_new = 5
    stats.stored_updated = 7
    stats.dedup_skipped = 11

    payload = stats.to_dict()

    assert len(payload) == 28, "23 keys before, 28 after"
    assert {name: payload[name] for name in FIELDS} == {
        "incremental_hits": 3,
        "incremental_misses": 2,
        "stored_new": 5,
        "stored_updated": 7,
        "dedup_skipped": 11,
    }


def test_custom_stats_stays_clean():
    stats = CrawlStats()

    for name in FIELDS:
        setattr(stats, name, 1)

    assert stats.custom_stats == {}, "these counters have real fields; nothing should drift into custom_stats"
    assert not set(FIELDS) & set(stats.to_dict()["custom_stats"])


def test_two_instances_share_nothing():
    first, second = CrawlStats(), CrawlStats()

    first.custom_stats["x"] = 1
    first.proxies.append("http://p:1")
    first.response_status_count["status_200"] = 1
    first.autothrottle_delays["a.com"] = 0.5
    first.sessions_requests_count["sid"] = 1
    first.log_levels_counter["info"] = 1
    first.domains_response_bytes["a.com"] = 10
    first.stored_new += 1

    assert second.custom_stats == {}
    assert second.proxies == []
    assert second.response_status_count == {}
    assert second.autothrottle_delays == {}
    assert second.sessions_requests_count == {}
    assert second.log_levels_counter == {}
    assert second.domains_response_bytes == {}
    assert second.stored_new == 0
