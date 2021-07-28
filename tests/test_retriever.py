import pytest
import lib.cache
import lib.retriever
import lib.settings
import feedparser


class TestRetriever:
    def test_basic_retrieval(self):
        r = lib.retriever.RSSRetriever()
        result: feedparser.util.FeedParserDict = r.get(
            "https://www.ss.com/lv/real-estate/flats/liepaja-and-reg/liepaja/sell/rss/"
        )
        assert result.bozo is False
        assert result.status == 200
        assert isinstance(result.entries, list)

    def test_retrieval_with_cache(self):
        settings = lib.settings.Settings("settings.test.json")
        settings.data_cache = "retrieval_cache.db"
        cache = lib.cache.DataCache(settings)
        r = lib.retriever.RSSRetriever(cache)
        result: feedparser.util.FeedParserDict = r.get(
            "https://www.ss.com/lv/real-estate/flats/liepaja-and-reg/liepaja/sell/rss/"
        )
        assert result.bozo is False
        assert result.status == 200
        assert isinstance(result.entries, list)
