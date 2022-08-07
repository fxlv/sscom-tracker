import datetime
import hashlib
import os
import pickle
from pathlib import Path
from re import A
from typing import Iterator

import arrow
from loguru import logger

import lib.settings
from lib.helpers import shorthash
from lib.log import func_log
from lib.stats import TrackerStatsSql
from lib.store import Store
from lib.zabbix import Zabbix, ZabbixStopwatch


class RSSStore(Store):
    def __init__(self, settings: lib.settings.Settings):
        self.s = settings
        self.cache_dir = self.s.cache_dir
        self._create_cache_dir_if_not_exists()
        self.stats = TrackerStatsSql(self.s)
        self.l = logger.bind(task="RSSStore")
        self.l.trace("Initialized")
        self.zabbix = Zabbix(settings)

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
    def write_classified(self, url, data):
        # check the settings to determine write path
        full_path = self._get_full_file_name(url)
        self._create_dir_if_not_exists(full_path)
        file_handle = full_path.open(mode="wb")
        pickle.dump(data, file_handle)
        file_handle.close()
        self.stats.set_last_rss_update(arrow.now().datetime)

    def __del__(self):
        self.l.trace("RSS destructor triggering RSS files count update")
        self.stats.set_rss_files_count(self.get_files_count())

    def _file_is_not_empty(self, file_name: Path):
        return file_name.stat().st_size > 0

    def get_all_files(self):
        self.l.trace("Get all files")
        return Path(self.s.cache_dir).glob("*/*/*/*/*.rss")

    def get_latest_files(self):
        all_files = self.get_all_files()
        # return files created within the last day only
        now = arrow.now()
        for file in all_files:
            file_creation_time = arrow.get(file.stat().st_ctime)
            delta = now - file_creation_time
            if delta.days < 1: # return files less than 24h old
                yield file

    def get_files_count(self) -> int:
        return sum(1 for i in self.get_all_files())

    def load(self, all=False) -> Iterator[object]:
        load_stopwatch = ZabbixStopwatch(self.zabbix, "rss_files_load_time_seconds")

        self.l.trace("Load all")
        if all:
            all_files = self.get_all_files()
        else:
            all_files = self.get_latest_files()
        all_files_count = 0
        for file_name in all_files:
            if self._file_is_not_empty(file_name):
                logger.debug(f"Opening file {file_name} for binary reading")
                object = pickle.load(file_name.open(mode="rb"))
                all_files_count += 1
                yield object
        self.l.debug(f"{all_files_count} RSS files loaded")
        load_stopwatch.done()
