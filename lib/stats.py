import lib.settings
from lib.objectstore import ObjectStore
from lib.rssstore import RSSStore


class TrackerStats:
    def __init__(self, settings: lib.settings.Settings, object_store: ObjectStore, rss_store: RSSStore):
        self.categories = settings.tracking_list.keys()
        self.objects_count = object_store.get_files_count()
        self.rss_files_count = rss_store.get_files_count()
        self.object_store = object_store
        self.rss_store = rss_store

    def get_object_count(self, category):
        return self.object_store.get_files_count(category)