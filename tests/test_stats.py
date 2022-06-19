import datetime
import time
import arrow
import pytest

import lib.settings
import lib.stats

RSS_FILES_COUNT = 5

def stats_pickle():
    settings = lib.settings.Settings("settings.test.json")
    stats = lib.stats.TrackerStatsPickle(settings)
    return stats

def stats_sql():
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

@pytest.mark.parametrize("stats", [stats_pickle(), stats_sql()], ids=["pickle", "sql"])
def test_set_get_last_rss_update(stats):
    now = arrow.now()
    stats.set_last_rss_update(now.datetime)
    assert stats.get_last_rss_update() == now



@pytest.mark.parametrize("stats", [stats_pickle(), stats_sql()], ids=["pickle", "sql"])
def test_set_get_rss_files_count(stats):
    stats.set_rss_files_count(RSS_FILES_COUNT)
    assert stats.get_rss_files_count() == RSS_FILES_COUNT


@pytest.mark.parametrize("stats", [stats_pickle(), stats_sql()], ids=["pickle", "sql"])
def test_set_get_objects_files_count(stats):
    stats.set_objects_files_count("test", 7)
    assert stats.get_objects_files_count("test") == 7


@pytest.mark.parametrize("stats", [stats_pickle(), stats_sql()], ids=["pickle", "sql"])
def test_set_get_http_data_stats(stats):
    stats.set_http_data_stats(1, 1)
    assert stats.get_http_data_stats() == (1, 1)



@pytest.mark.parametrize("stats", [stats_pickle(), stats_sql()], ids=["pickle", "sql"])
def test_set_get_enrichment_stats(stats):
    stats.set_enrichment_stats(1, 1)
    assert stats.get_enrichment_stats() == (1, 1)


@pytest.mark.parametrize("stats", [stats_pickle(), stats_sql()], ids=["pickle", "sql"])
def test_set_last_objects_update(stats):
    stats.set_last_objects_update()
    now = arrow.now()
    delta = now - stats.get_last_objects_update()
    assert delta.total_seconds() < 1

@pytest.mark.parametrize("stats", [stats_pickle(), stats_sql()], ids=["pickle", "sql"])
def test_get_last_objects_update(stats):
    stats.set_last_objects_update()
    last = stats.get_last_objects_update()
    now = arrow.now()
    delta = now - last
    assert delta.total_seconds() < 1

@pytest.mark.parametrize("stats", [stats_pickle(), stats_sql()], ids=["pickle", "sql"])
def test_last_objects_update_throttling_works(stats):
    """Last objects update is throttled to once per minute.
    
    In order to avoid spamming the database we set last 
    object update not more than once per minute.
    """
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

# TODO:
# update test parameters to set random data and also to set invorrect data and catch expceptions
# this way - validating what should be the behavior when incorrect data is passed
# as currently that is not defined behavior at all

# TODO: write tests that verify types of timestamp. meaning that if it is arrow or vanilla datetime
# how should the stats methods react