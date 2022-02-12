import datetime
import os
import pickle
from pathlib import Path

from loguru import logger
import arrow
import lib.datastructures
import lib.settings
from lib.datastructures import Apartment, Classified
from lib.store import ObjectStore
from lib.stats import TrackerStats

import sqlite3


def get_object_store(storage_type: str):
    if storage_type == "files":
        return ObjectStoreFiles
    elif storage_type == "sqlite":
        return ObjectStoreSqlite
    else:
        raise ValueError("Unsupported storage type")


class ObjectStoreSqlite(ObjectStore):
    # create tables with:
    # create table apartments (hash text, short_hash text, title text, rooms int, floor int, price int, street text, enriched bool, published timestamp );
    #
    def __init__(self, settings: lib.settings.Settings):
        self.s = settings
        self.con = sqlite3.connect(self.s.sqlite_db)
        self.cur = self.con.cursor()
        logger.trace("ObjectStoreSqlite ready")

    def get_classified_count(self, category) -> int:
        pass

    def _get_classified_apartment(
        self, hash_string: str
    ) -> lib.datastructures.Apartment:
        self.cur.execute("select * from apartments where hash = '%s'" % hash_string)
        results = self.cur.fetchall()
        if len(results) == 0:
            return None
        elif len(results) == 1:
            result = results[0]
            apartment = lib.datastructures.Apartment(title=result[2], street=result[6])
            apartment.hash = result[0]
            apartment.short_hash = result[1]
            apartment.rooms = result[3]
            apartment.floor = result[4]
            apartment.enriched = result[7]
            apartment.published = arrow.get(result[8])
            return apartment
        else:
            raise Exception("Unexpected number of database results returned")

    def get_classified(self, classified: lib.datastructures.Classified) -> Classified:
        if classified.category == "apartment":
            return self._get_classified_apartment(classified.hash)
        else:
            raise ValueError("Unsupported classified category")

    def get_classified_by_category_hash(self, category, hash_string) -> Classified:
        pass

    def _is_valid_category(self, category_string) -> bool:
        valid_categories = ["apartment", "car", "house", "dog", "*"]
        return category_string in valid_categories
   
    def _create_apartment_from_db_result(self, result) -> Apartment:
        """Takes a tuple and returns an instance of Apartment"""
        a = Apartment(result[2], result[6])
        a.hash = result[0]
        a.short_hash = result[1]
        a.published = result[8]
        a.floor = result[4]
        a.rooms = result[3]
        return a

    def _get_all_apartments(self) -> list[Apartment]:
        self.cur.execute("select * from apartments")
        results = self.cur.fetchall()
        apartments_list = []
        for r in results:
            apartments_list.append(self._create_apartment_from_db_result(r))
        return apartments_list

    def get_all_classifieds(self, category="*") -> list:
        """Returns a list of all classifieds."""
        if not self._is_valid_category(category):
            raise ValueError("Invalid category specified")
        if category == "apartment":
            return self._get_all_apartments()
        else:
            raise NotImplementedError()



    def _write_classified_apartment(self, apartment: lib.datastructures.Apartment):
        sql = f"insert or replace into apartments values('{apartment.hash}', '{apartment.short_hash}', '{apartment.title}', '{apartment.rooms}', '{apartment.floor}', '{apartment.price}', '{apartment.street}', '{apartment.enriched}', '{apartment.published}')"
        self.cur.execute(sql)
        self.con.commit()

    def write_classified(self, classified: lib.datastructures.Classified):
        if classified.category == "apartment":
            self._write_classified_apartment(classified)
            return True
        else:
            raise ValueError("Unsupported classified category")
    

    def _update_classified_apartment(self, apartment: Apartment) -> bool:
        return self._write_classified_apartment(apartment)





    def update_classified(self, classified: Classified):
        if classified.category == "apartment":
            return self._update_classified_apartment(classified)
        else:
            raise NotImplementedError()


class ObjectStoreFiles(ObjectStore):
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

    def update_classified(self, classified: Classified):
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
            classified = self.get_classified(classified)
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

    def get_classified(
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

    def _get_all_files(self, category):
        all_files = Path(self.s.object_cache_dir).glob(f"{category}/*.classified")
        return all_files

    def get_all_classifieds(self, category="*") -> list:
        """Returns a list of all classifieds."""
        all_files = self._get_all_files(category)
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

    def get_classified_by_category_hash(
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

    def get_classified_count(self, category="*") -> int:
        return sum(1 for i in self._get_all_files(category))

    def __del__(self):
        total = 0
        for category in self.stats.data.categories:
            count = self.get_classified_count(category)
            total += count
            self.stats.set_objects_files_count(category, count)
        self.stats.set_objects_files_count("total", total)
