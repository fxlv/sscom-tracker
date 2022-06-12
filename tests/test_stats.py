import datetime

import arrow
import pytest

import lib.settings
import lib.stats

RSS_FILES_COUNT = 5

@pytest.fixture
def stats():
    settings = lib.settings.Settings("settings.test.json")
    stats = lib.stats.TrackerStats(settings)
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

def test_set_last_rss_update(stats):
    now = datetime.datetime.now()
    stats.set_last_rss_update(now)
    assert stats.data.last_rss_update == now

def test_get_last_rss_update(stats):
    # requires set_last_rss_update to be called before
    last = stats.get_last_rss_update()
    now = datetime.datetime.now()
    delta = now - last
    assert delta.total_seconds() < 1


def test_set_get_rss_files_count(stats):
    stats.set_rss_files_count(RSS_FILES_COUNT)
    assert stats.get_rss_files_count() == RSS_FILES_COUNT


def test_set_get_objects_files_count(stats):
    stats.set_objects_files_count("test", 7)
    assert stats.get_objects_files_count("test") == 7


def test_set_get_http_data_stats(stats):
    stats.set_http_data_stats(1, 1)
    assert stats.get_http_data_stats() == (1, 1)



def test_set_enrichment_stats(stats):
    stats.set_enrichment_stats(1, 1)
    assert stats.data.enrichment_data == (1, 1)

def test_get_enrichment_stats(stats):
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

