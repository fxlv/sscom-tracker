import datetime
import os
import pickle

from loguru import logger

import lib.settings
from lib.log import func_log, set_up_logging, normalize


class Cache:
    """Generic cache class."""

    def __init__(self, settings: lib.settings.Settings, local_cache=None) -> None:
        """Construct the cache object."""

        lib.log.set_up_logging(settings, debug=True)
        self.logger = logger.bind(task="Cache")

        if local_cache is None:
            # use the cache as specified in settings
            self.local_cache = settings.local_cache
            self.logger.debug("Using cache file specified in settings")
        else:
            # use the cache specified in constructor arguments
            self.local_cache = local_cache
            self.logger.debug("Using cache file from constructor arguments")
        self.settings = settings

        self.cache = None
        if not self.load_cache_from_disk():
            self.create_new_cache()

    def load_cache_from_disk(self) -> bool:
        """Load cache from pickle file."""
        if not os.path.exists(self.local_cache):
            return False
        self.logger.debug("Loading cache file from disk")
        cache_file = open(self.local_cache, "rb")
        self.cache = pickle.load(cache_file)
        return True

    def create_new_cache(self):
        """Initialize new cache object."""
        self.logger.debug("Creating a new cache object")
        self.cache = []

    def __del__(self) -> None:
        """Save cache upon destruction."""
        self.logger.debug(f"Destructor called for Cache object {self}")
        self.save()

    def add(self, item: object) -> bool:
        """Add an item to the cache."""
        self.logger.debug(f"Adding item {str(item)[:20]} to cache")
        return self.cache.append(item)

    def is_known(self, item: object) -> bool:
        """Return True if object is in cache."""
        return item in self.cache

    def save(self) -> None:
        """Save cache to pickle file."""
        with open(self.local_cache, "wb") as cache_file:
            pickle.dump(self.cache, cache_file)
            self.logger.debug(f"Cache saved to file: {self.local_cache}")

    def cache_file_exists(self) -> bool:
        if not os.path.exists(self.local_cache):
            self.logger.debug(f"Local cache {self.local_cache} does not exist")
            return False
        return True


class DataCache(Cache):
    """Data Cache class for storing html response data."""

    def __init__(self, settings: lib.settings.Settings) -> None:
        """Constructor calls parent and overrides local_cache."""
        Cache.__init__(self, settings, local_cache=settings.data_cache)

    def __contains__(self, item):
        """For the membership operator."""
        return item in self.cache["data"].keys()

    def create_new_cache(self):
        """Initialize new cache object."""
        self.cache = {"data": {}, "last_update": None}

    def get_timestamp(self) -> datetime:
        return self.cache["last_update"]

    def get(self, key: str) -> object:
        """Returns cache object."""
        return self.cache["data"][key]

    def is_fresh(self):
        """Return True if cache is fresh.

        Returns True if cache age in seconds is less than defined in 'cache_freshness' variable in settings.
        """
        if not self.cache_file_exists():
            return False
        current_timestamp = datetime.datetime.now()
        cache_timestamp = self.get_timestamp()
        delta = current_timestamp - cache_timestamp
        delta_seconds = delta.total_seconds()
        if delta_seconds > self.settings.cache_validity_time:
            logger.debug(f"Cache is not fresh. Delta: {delta_seconds} seconds")
            return False
        # if cache is fresh, continue
        logger.debug(f"Cache is fresh. Delta: {delta_seconds} seconds")
        return True

    def add(self, key: str, item: object) -> None:
        """Add an item to the cache."""
        self.cache["data"][key] = item
        self.cache["last_update"] = datetime.datetime.now()

    def is_known(self, key: str) -> bool:
        """Return True if key is in cache."""
        return key in self.cache["data"].keys()
