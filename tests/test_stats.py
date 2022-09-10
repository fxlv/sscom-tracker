import datetime
import time
import arrow
import pytest
from hypothesis import given, strategies as st
import lib.settings
import lib.stats

RSS_FILES_COUNT = 5

@pytest.fixture(scope="module")
def stats():
    settings = lib.settings.Settings("settings.test.json")
    stats = lib.stats.TrackerStatsSql(settings)
    return stats


def test_stats_data():
    data = lib.stats.StatsData()
    assert data.objects_files_count == {}
    assert data.rss_files_count is None
    assert data.last_rss_update is None
    assert data.last_objects_update is None
    assert data.last_retrieval_run is None
    assert data.last_enricher_run is None
    assert data.categories == []
    assert data.http_data == (None, None)
    assert data.enrichment_data == (None, None)

def test_set_get_last_rss_update(stats):
    now = arrow.now()
    stats.set_last_rss_update(now.datetime)
    assert stats.get_last_rss_update() == now



def test_set_get_rss_files_count(stats):
    stats.set_rss_files_count(RSS_FILES_COUNT)
    assert stats.get_rss_files_count() == RSS_FILES_COUNT

@given(files_count = st.integers(max_value=9223372036854775806, min_value=0))
def test_set_get_objects_files_count(stats, files_count):
    stats.set_objects_files_count("test", files_count)
    assert stats.get_objects_files_count("test") == files_count


#@pytest.mark.parametrize("stats", [stats_pickle(), stats_sql()], ids=["pickle", "sql"])
@given(total_files_count = st.integers(max_value=9223372036854775806, min_value=0),
    files_with_http_data=st.integers(max_value=9223372036854775806, min_value=0))
def test_set_get_http_data_stats(stats, total_files_count, files_with_http_data):
    stats.set_http_data_stats(total_files_count, files_with_http_data)
    assert stats.get_http_data_stats() == (total_files_count, files_with_http_data)

def test_negative_integers_throw_exceptions(stats):
    with pytest.raises(ValueError):
        stats.set_objects_files_count("test", -1)
    with pytest.raises(ValueError):
        stats.set_http_data_stats(-1, -1)
    with pytest.raises(ValueError):
        stats.set_enrichment_stats(-1, -1)
    with pytest.raises(ValueError):
        stats.set_rss_files_count(-1)

def test_non_timestamp_input_throws_exceptions(stats):
    with pytest.raises(ValueError):
        stats.set_last_rss_update(-1)


def test_set_get_enrichment_stats(stats):
    stats.set_enrichment_stats(1, 1)
    assert stats.get_enrichment_stats() == (1, 1)


def test_set_last_objects_update(stats):
    stats.set_last_objects_update()
    now = arrow.now()
    delta = now - stats.get_last_objects_update()
    assert delta.total_seconds() < 1

def test_get_last_objects_update(stats):
    stats.set_last_objects_update()
    last = stats.get_last_objects_update()
    now = arrow.now()
    delta = now - last
    assert delta.total_seconds() < 1

def test_last_objects_update_throttling_works(stats):
    """Last objects update is throttled to once per minute.

    In order to avoid spamming the database we set last
    object update not more than once per minute.
    """
    stats.last_objects_update_timestamp = None #reste throttling to default state
    now = arrow.now()
    stats.set_last_objects_update()
    # at this point it is set and next updates should be noops
    time.sleep(2)
    stats.set_last_objects_update()
    # despite the fact that it is one second later,
    # if we query for stats object update, it should return the initial value we set above
    last = stats.get_last_objects_update()
    delta = last - now
    # subtracting last update from "now" should be 0 in terms of seconds
    # if it is negative, then it means throttling did not work
    assert delta.total_seconds() >= 0
    assert delta.total_seconds() < 1
