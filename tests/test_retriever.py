import pytest
import lib.cache
import lib.objectparser
import lib.objectstore
import lib.retriever
import lib.rssstore
import lib.settings
import feedparser
import os


class TestRSSStorage:
    def test_load_all(self):
        settings = lib.settings.Settings("settings.test.json")
        store = lib.rssstore.RSSStore(settings)
        op = lib.objectparser.ObjectParser()
        object_store = lib.objectstore.ObjectStoreFiles(settings)
        objects_list = store.load_all()
        for rss_object in objects_list:
            parsed_list = op.parse_object(rss_object)
            for classified in parsed_list:
                object_store.write_classified(classified)
        object_store.load_all()


class TestRetriever:
    @pytest.fixture(scope="class")
    def chdir(self, request):
        print(f"dirname: {request.fspath.dirname}")
        print(f"invocation dir: {request.config.invocation_dir}")
        os.chdir(request.fspath.dirname)
        yield
        os.chdir(request.config.invocation_dir)

    def test_basic_retrieval(self):
        r = lib.retriever.RSSRetriever()
        result: feedparser.util.FeedParserDict = r.get(
            "https://www.ss.com/lv/real-estate/flats/liepaja-and-reg/liepaja/sell/rss/"
        )
        assert isinstance(result, feedparser.FeedParserDict)
        assert result.bozo is False
        assert result.status == 200
        assert isinstance(result.entries, list)


class TestRetrieverManager:
    def test_retrieve_many(self):
        settings = lib.settings.Settings("settings.test.json")
        rm = lib.retriever.RetrieverManager(settings)
        rm.update_all("*")
        # TODO: come up with a test scenario
