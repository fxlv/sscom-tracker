import datetime

import arrow
import pytest

import lib.settings
import lib.stats


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


def test_set_rss_files_count(stats):
    now = datetime.datetime.now()
    stats.set_rss_files_count(now)
    assert stats.data.rss_files_count == now


def test_set_objects_files_count(stats):
    now = datetime.datetime.now()
    stats.set_objects_files_count("test", now)
    assert stats.data.objects_files_count["test"] == now


def test_set_http_data_stats(stats):
    stats.set_http_data_stats(1, 1)
    assert stats.data.http_data == (1, 1)


def test_set_enrichment_stats(stats):
    stats.set_enrichment_stats(1, 1)
    assert stats.data.enrichment_data == (1, 1)


def test_set_last_objects_update(stats):
    stats.set_last_objects_update()
    now = arrow.now()
    delta = now - stats.data.last_objects_update
    assert delta.total_seconds() < 1
