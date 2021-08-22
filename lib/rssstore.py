import datetime
import hashlib
import os
import pickle
from pathlib import Path

import arrow
from loguru import logger

import lib.settings
from lib.helpers import shorthash
from lib.log import func_log
from lib.store import Store
from lib.stats import TrackerStats


class RSSStore(Store):
    def __init__(self, settings: lib.settings.Settings):
        self.s = settings
        self.cache_dir = self.s.cache_dir
        self._create_cache_dir_if_not_exists()
        self.stats = TrackerStats(self.s)
        self.l = logger.bind(task="RSSStore")

    @func_log
    def _hash(self, string: str):
        """Hash a string and return a hex digest."""
        return hashlib.sha256(string.encode("utf-8")).hexdigest()

    @func_log
    def _get_full_file_name(self, url, file_date=datetime.datetime.now()) -> Path:
        file_name = self._hash(url)
        full_path = Path(
            f"{self.cache_dir}/{file_date.year}/{file_date.month}/{file_date.day}/{file_date.hour}/{file_name}.rss"
        )
        return full_path

    @func_log
    def _create_cache_dir_if_not_exists(self):
        return self._create_dir_if_not_exists(self.cache_dir)

    @func_log
    def _create_dir_if_not_exists(self, path_name):
        p = Path(path_name).absolute()
        os.makedirs(p.parent, exist_ok=True)

    @func_log
    def fresh_cache_present(self, url):
        full_path = self._get_full_file_name(url)
        now = arrow.now()
        try:
            mtime = arrow.get(full_path.stat().st_mtime)
            delta_seconds = (now - mtime).total_seconds()
            return delta_seconds < self.s.cache_validity_time
        except FileNotFoundError:
            self.l.debug(f"[{shorthash(url)}] Cache file not present for {url}")
            return False

    @func_log
    def write(self, url, data):
        # check the settings to determine write path
        full_path = self._get_full_file_name(url)
        self._create_dir_if_not_exists(full_path)
        file_handle = full_path.open(mode="wb")
        pickle.dump(data, file_handle)
        file_handle.close()
        self.stats.set_last_rss_update(arrow.now())

    def __del__(self):
        self.l.trace("RSS destructor triggering RSS files count update")
        self.stats.set_rss_files_count(self.get_files_count())

    def _file_is_not_empty(self, file_name: Path):
        return file_name.stat().st_size > 0

    def get_all_files(self):
       self.l.trace("Get all files")
       return Path(self.s.cache_dir).glob("*/*/*/*/*.rss")

    def get_files_count(self)  -> int:
        return sum( 1 for i in self.get_all_files())

    def load_all(self):
        all_files = self.get_all_files()
        all_files_unpickled = []
        for file_name in all_files:
            if self._file_is_not_empty(file_name):
                logger.debug(f"Opening file {file_name} for binary reading")
                object = pickle.load(file_name.open(mode="rb"))
                all_files_unpickled.append(object)
        file_count = len(all_files_unpickled)
        self.l.debug(f"{file_count} RSS files loaded")
        return all_files_unpickled