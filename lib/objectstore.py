import datetime
import os
import pickle
from pathlib import Path

from loguru import logger
import arrow
import lib.datastructures
import lib.settings
from lib.log import func_log
from lib.store import Store
from lib.stats import TrackerStats


class ObjectStoreFiles(Store):
    def __init__(self, settings: lib.settings.Settings):
        self.s = settings
        self.object_cache_dir = self.s.object_cache_dir
        self._create_cache_dir_if_not_exists()
        self.stats = TrackerStats(self.s)
        logger.trace("Object store ready")

    def _create_cache_dir_if_not_exists(self):
        return self._create_dir_if_not_exists(self.object_cache_dir)

    def _file_is_not_empty(self, file_name: Path):
        return file_name.stat().st_size > 0

    def _create_dir_if_not_exists(self, path_name):
        p = Path(path_name).absolute()
        os.makedirs(p.parent, exist_ok=True)

    def update(self, classified: lib.datastructures.Classified) -> object:
        # check the settings to determine write path
        now = datetime.datetime.now()
        if self._file_exists(classified):
            logger.debug(f"[{classified.short_hash}] updating...")
            full_path = self._get_full_file_name(classified)
            if self._file_is_not_empty(full_path):
                file_handle = full_path.open(mode="wb")
                pickle.dump(classified, file_handle)
                file_handle.close()
            else:
                logger.warning(f"File {full_path} is empty!")
        else:
            logger.warning(
                f"[{classified.short_hash}] classified does not exist, cannot update it."
            )

    def write_classified(self, classified: lib.datastructures.Classified):
        # check the settings to determine write path
        now = datetime.datetime.now()
        if self._file_exists(classified):
            # such classified is already knonw, therefore, instead of overwriting it blindly
            # we will load it from cache, and update the 'last_seen' date and then write it back
            # this way, we maintain the "first seen" timestamp
            classified = self.load_classified(classified)
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
        self.stats.set_last_objects_update(arrow.now())
        return True

    def load_classified(
        self, classified: lib.datastructures.Classified
    ) -> lib.datastructures.Classified:
        # check the settings to determine write path
        if not self._file_exists(classified):
            return None
        full_path = self._get_full_file_name(classified)
        file_handle = full_path.open(mode="rb")
        logger.trace(
            f"[{classified.short_hash}] Opened handle {file_handle} for binary reading"
        )
        if self._file_is_not_empty(full_path):
            loaded_file = pickle.load(file_handle)
        else:
            logger.warning(f"Cannot load an empty file!")
            return None
        logger.debug(f"[{classified.short_hash}] Loaded from disk")
        return loaded_file

    def get_all_files(self, category):
        all_files = Path(self.s.object_cache_dir).glob(f"{category}/*.classified")
        return all_files

    def load_all(self, category="*") -> list:
        all_files = self.get_all_files(category)
        all_files_unpickled = []
        for file_name in all_files:
            logger.trace(f"Loading file {file_name}...")
            if self._file_is_not_empty(file_name):
                object = pickle.load(file_name.open(mode="rb"))
                all_files_unpickled.append(object)
            else:
                logger.warning(f"Cannot load an empty file!")
        file_count = len(all_files_unpickled)
        logger.debug(f"{file_count} files were read and unpickled")
        # sort by date published, desc
        all_files_unpickled.sort(key=lambda x: x.published, reverse=True)
        return all_files_unpickled

    def get_object_by_hash(
        self, category: str, hash_string: str
    ) -> lib.datastructures.Classified:
        file_path = Path(self.s.object_cache_dir).glob(
            f"{category}/{hash_string}*.classified"
        )
        return pickle.load(next(file_path).open(mode="rb"))

    def _file_exists(self, classified: lib.datastructures.Classified) -> bool:
        return self._get_full_file_name(classified).exists()

    def _get_full_file_name(self, classified) -> Path:
        file_name = classified.hash
        full_path = Path(
            f"{self.object_cache_dir}/{classified.category}/{file_name}.classified"
        )
        return full_path

    def get_files_count(self, category="*") -> int:
        return sum(1 for i in self.get_all_files(category))

    def __del__(self):
        logger.trace("Destroying objectstore")
        total = 0
        for category in self.stats.data.categories:
            count = self.get_files_count(category)
            total += count
            self.stats.set_objects_files_count(category, count)
        self.stats.set_objects_files_count("total", total)
