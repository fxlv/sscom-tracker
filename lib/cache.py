import logging
import os
import pickle


class Cache:

    def __init__(self, settings: object) -> None:
        self.local_cache = settings["local_cache"]
        if not os.path.exists(self.local_cache):
            logging.debug("Local cache does not exist, will create a new one.")
            self.cache = []
        else:
            cache_file = open(self.local_cache, "rb")
            self.cache = pickle.load(cache_file)

    def __del__(self):
        logging.debug("Destructor called...")
        self.save()

    def add(self, h) -> bool:
        """Store a hash in local cache"""
        return self.cache.append(h)

    def is_known(self, ad) -> bool:
        """Check hash against local cache"""
        h = ad.get_hash()
        if h in self.cache:
            return True
        else:
            self.add(h)
            return False

    def save(self):
        with open(self.local_cache, "wb") as cache_file:
            pickle.dump(self.cache, cache_file)
            logging.debug("Cache file saved")
