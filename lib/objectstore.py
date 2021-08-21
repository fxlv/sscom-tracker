import datetime
import os
import pickle
from pathlib import Path

from loguru import logger

import lib.datastructures
import lib.settings
from lib.log import func_log
from lib.store import Store


class ObjectStore(Store):
    def __init__(self, settings: lib.settings.Settings):
        self.s = settings
        self.object_cache_dir = self.s.object_cache_dir
        self._create_cache_dir_if_not_exists()

    @func_log
    def _create_cache_dir_if_not_exists(self):
        return self._create_dir_if_not_exists(self.object_cache_dir)

    @func_log
    def _create_dir_if_not_exists(self, path_name):
        p = Path(path_name).absolute()
        os.makedirs(p.parent, exist_ok=True)

    @func_log
    def write(self, classified: lib.datastructures.Classified):
        # check the settings to determine write path
        now = datetime.datetime.now()
        if self._file_exists(classified):
            # such classified is already knonw, therefore, instead of overwriting it blindly
            # we will load it from cache, and update the 'last_seen' date and then write it back
            # this way, we maintain the "first seen" timestamp
            classified = self.load(classified)
            classified.last_seen = now
            logger.debug(
                f"[{classified.short_hash}] classified is known, updating the 'last_seen' time"
            )
        else:
            classified.first_seen = now
            classified.last_seen = now
            logger.debug(f"[{classified.short_hash}] new classified")
        full_path = self._get_full_file_name(classified)
        self._create_dir_if_not_exists(full_path)
        file_handle = full_path.open(mode="wb")
        pickle.dump(classified, file_handle)
        file_handle.close()

    @func_log
    def load(self, classified: lib.datastructures.Classified):
        # check the settings to determine write path
        if not self._file_exists(classified):
            return None
        full_path = self._get_full_file_name(classified)
        file_handle = full_path.open(mode="rb")
        logger.trace(f"[{classified.short_hash}] Opened handle {file_handle} for binary reading")
        logger.debug(f"[{classified.short_hash}] Loaded from disk")
        return pickle.load(file_handle)


    @func_log
    def get_all_files(self, category):
        all_files = Path(self.s.object_cache_dir).glob(f"{category}/*.classified")
        return all_files

    @func_log
    def load_all(self, category="*"):
        all_files = self.get_all_files(category)
        all_files_unpickled = []
        for file_name in all_files:
            object = pickle.load(file_name.open(mode="rb"))
            all_files_unpickled.append(object)
        file_count = len(all_files_unpickled)
        logger.debug(f"{file_count} files were read and unpickled")
        return all_files_unpickled

    @func_log
    def _file_exists(self, classified: lib.datastructures.Classified) -> bool:
        return self._get_full_file_name(classified).exists()

    @func_log
    def _get_full_file_name(self, classified) -> Path:
        file_name = classified.hash
        full_path = Path(
            f"{self.object_cache_dir}/{classified.category}/{file_name}.classified"
        )
        return full_path

    @func_log
    def get_files_count(self, category="*")  -> int:
        return sum( 1 for i in self.get_all_files(category))