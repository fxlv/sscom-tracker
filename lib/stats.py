import lib.settings
from pathlib  import Path
from loguru import logger
import pickle
import datetime


class StatsData:
    def __init__(self):
        self.objects_files_count = {}
        self.rss_files_count = None
        self.last_rss_update = None
        self.last_objects_update = None
        self.categories = []


class TrackerStats:
    def __init__(self, settings: lib.settings.Settings):
        self.settings = settings
        self.data: StatsData = self.load()
        self.data.categories = list(self.settings.tracking_list.keys())

    #def __del__(self):
     #   with logger.contextualize(task="Stats->Destructor"):
      #      logger.trace("Stats destructing, saving...")
            #self.save()

    def set_last_rss_update(self, timestamp: datetime.datetime):
        self.data.last_rss_update = timestamp
        self.save()

    def set_rss_files_count(self, count: int):
        self.data.rss_files_count = count
        self.save()

    def set_objects_files_count(self, category:str, count: int):
        self.data.objects_files_count[category] = count
        self.save()

    def set_last_objects_update(self, timestamp: datetime.datetime):
        self.data.last_objects_update = timestamp
        self.save()

    def gen_stats(self, object_store, rss_store) :
        with logger.contextualize(task="Stats->Gen stats"):
            logger.trace("Generating stats")
            self.object_store = object_store
            self.rss_store = rss_store
            self.data.objects_count = object_store.get_files_count()
            self.data.rss_files_count = rss_store.get_files_count()

    def _get_stats_file(self):
        with logger.contextualize(task="Stats"):
            stats_file = Path(f"{self.settings.cache_dir}/stats.db")
            logger.trace(f"Returning stats file {stats_file}")
            return stats_file

    def save(self):
        with logger.contextualize(task="Stats->Save"):
            logger.trace("Saving stats to pickle file")
            fh = open(self._get_stats_file(),"wb")
            pickle.dump(self.data, fh)
            logger.trace("Saved stats to pickle file")
            fh.close()

    def load(self):
        with logger.contextualize(task="Stats->Load"):
            logger.trace("Loaded stats from pickle file")
            stats_file = self._get_stats_file()
            if not stats_file.exists():
                logger.warning("Stats file did not exist, using empty stats")
                return StatsData() # return a fresh instance in case nothing exists on disk
            else:
                fh = open(stats_file,"rb")
                return pickle.load(fh)


    def get_object_count(self, category):
        return self.object_store.get_files_count(category)