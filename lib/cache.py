import logging
import os
import pickle
import lib.datastructures
import datetime

class Cache:
    """Generic cache class."""

    def __init__(self, settings: object, local_cache=None) -> None:
        """Construct the cache object."""

        if local_cache is None:
            # use the cache as specified in settings
            self.local_cache = settings["local_cache"]
            logging.debug("Using cache file specified in settings")
        else:
            # use the cache specified in constructor arguments
            self.local_cache = local_cache
            logging.debug("Using cache file from constructor arguments")

        self.cache = None
        if not self.load_cache_from_disk():
            self.create_new_cache()


    def load_cache_from_disk(self) -> bool:
        """Load cache from pickle file."""
        if not os.path.exists(self.local_cache):
            return False
        logging.debug("Loading cache file from disk")
        cache_file = open(self.local_cache, "rb")
        self.cache = pickle.load(cache_file)
        return True


    def create_new_cache(self):
        """Initialize new cache object."""
        logging.debug("Creating a new cache object")
        self.cache = []


    def __del__(self) -> None:
        """Save cache upon destruction."""
        logging.debug("Destructor called for Cache object {}".format(self))
        self.save()


    def add(self, item: object) -> bool:
        """Add an item to the cache."""
        logging.debug("Adding item {}... to cache".format(str(item)[:20]))
        return self.cache.append(item)


    def is_known(self, item: object) -> bool:
        """Return True if object is in cache."""
        return item in self.cache


    def save(self) -> None:
        """Save cache to pickle file."""
        with open(self.local_cache, "wb") as cache_file:
            pickle.dump(self.cache, cache_file)
            logging.debug("Cache saved to file: {}".format(self.local_cache))


class DataCache(Cache):
    def __init__(self, settings: object) -> None:
        Cache.__init__(self,settings, local_cache=settings["data_cache"])

    def create_new_cache(self):
        """Initialize new cache object."""
        self.cache = {"data": {}, "last_update": None}

    def get_timestamp(self):
        return self.cache["last_update"]

    def get(self, key: str) -> object:
        return self.cache["data"][key]

    def add(self, key: str, item: object) -> None:
        """Add an item to the cache."""
        self.cache["data"][key] = item
        self.cache["last_update"] = datetime.datetime.now()

    def is_known(self, key: str) -> bool:
        """Return True if key is in cache."""
        return key in self.cache["data"].keys()
