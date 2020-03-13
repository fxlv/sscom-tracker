import os
import pickle


from tracker import local_cache


class Cache:

    def __init__(self):
        if not os.path.exists(local_cache):
            print("Local cache does not exist, will create a new one.")
            self.cache = []
        else:
            cache_file = open(local_cache,"rb")
            self.cache = pickle.load(cache_file)
    def __del__(self):
        print("Destructor called...")
        self.save()
    def add(self, h):
        """Store a hash in local cache"""
        return self.cache.append(h)

    def is_known(self, ad):
        """Check hash against local cache"""
        h = ad.get_hash()
        if h in self.cache:
            return True
        else:
            self.add(h)
            return False

    def save(self):
        with open(local_cache, "wb") as cache_file:
            pickle.dump(self.cache, cache_file)
            print("Cache file saved")