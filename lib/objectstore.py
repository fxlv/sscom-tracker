import datetime
import os
import pickle
from pathlib import Path

from loguru import logger
import arrow
import lib.datastructures
import lib.settings
from lib.datastructures import Apartment, Classified, House, Car
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
        self.stats = TrackerStats(self.s)
        logger.trace("ObjectStoreSqlite ready")

    def __del__(self):
        total = 0
        for category in self.stats.data.categories:
            count = self.get_classified_count(category)
            total += count
            self.stats.set_objects_files_count(category, count)
        self.stats.set_objects_files_count("total", total)

    def get_classified_count(self, category) -> int:
        if category == "apartment":
            return self._get_count_apartments()
        elif category == "house":
            return self._get_count_houses()
        elif category == "car":
            return self._get_count_cars()
        elif category == "dog":
            return self._get_count_dogs()
        else:
            raise ValueError("Unsupported classified category")

    def _get_count_dogs(self) -> int:
        self.cur.execute( "select count(*) from dogs"    )
        results = self.cur.fetchall()
        if len(results) == 1:
            result = results[0]
            return result[0]
        else:
            raise Exception("Unexpected result received from DB")
    def _get_count_cars(self) -> int:
        self.cur.execute( "select count(*) from cars"    )
        results = self.cur.fetchall()
        if len(results) == 1:
            result = results[0]
            return result[0]
        else:
            raise Exception("Unexpected result received from DB")
    def _get_count_houses(self) -> int:
        self.cur.execute( "select count(*) from houses"    )
        results = self.cur.fetchall()
        if len(results) == 1:
            result = results[0]
            return result[0]
        else:
            raise Exception("Unexpected result received from DB")
    def _get_count_apartments(self) -> int:
        self.cur.execute( "select count(*) from apartments"    )
        results = self.cur.fetchall()
        if len(results) == 1:
            result = results[0]
            return result[0]
        else:
            raise Exception("Unexpected result received from DB")

    def _get_classified_house(self, hash_string: str) -> lib.datastructures.Apartment:
        self.cur.execute("select * from houses where short_hash = '%s'" % hash_string)
        results = self.cur.fetchall()
        if len(results) == 0:
            return None
        elif len(results) == 1:
            result = results[0]
            return self._create_house_from_db_result(result)
        else:
            raise Exception("Unexpected number of database results returned")

    def _get_classified_apartment(
        self, hash_string: str
    ) -> lib.datastructures.Apartment:
        self.cur.execute("select * from apartments where short_hash = '%s'" % hash_string)
        results = self.cur.fetchall()
        if len(results) == 0:
            return None
        elif len(results) == 1:
            result = results[0]
            return self._create_apartment_from_db_result(result)
        else:
            raise Exception("Unexpected number of database results returned")

    def _get_classified_car(self, hash_string: str) -> lib.datastructures.Car:
        self.cur.execute("select * from cars where short_hash = '%s'" % hash_string)
        results = self.cur.fetchall()
        if len(results) == 0:
            return None
        elif len(results) == 1:
            result = results[0]
            return self._create_car_from_db_result(result)
        else:
            raise Exception("Unexpected number of database results returned")

    def get_classified(self, classified: lib.datastructures.Classified) -> Classified:
        return self.get_classified_by_category_hash(
            classified.category, classified.hash
        )

    def get_classified_by_category_hash(self, category, hash_string) -> Classified:
        if category == "apartment":
            return self._get_classified_apartment(hash_string)
        elif category == "house":
            return self._get_classified_house(hash_string)
        elif category == "car":
            return self._get_classified_car(hash_string)
        else:
            raise ValueError("Unsupported classified category")

    def _is_valid_category(self, category_string) -> bool:
        valid_categories = ["apartment", "car", "house", "dog", "*"]
        return category_string in valid_categories

    def _create_apartment_from_db_result(self, result) -> Apartment:
        """Takes a tuple and returns an instance of Apartment"""
        a = Apartment(result[3], result[7])
        a.hash = result[0]
        a.short_hash = result[1]
        a.link = result[2]
        a.published = arrow.get(result[9])
        a.floor = result[5]
        a.price = result[6]
        a.rooms = result[4]
        a.enriched = result[8]
        a.http_response_data = result[10]
        a.http_response_code = result[11]
        return a

    def _create_house_from_db_result(self, result) -> House:
        """Takes a tuple and returns an instance of House"""
        a = House(result[3], result[7])
        a.hash = result[0]
        a.short_hash = result[1]
        a.link = result[2]
        a.published = arrow.get(result[9])
        a.floor = result[5]
        a.rooms = result[4]
        a.price = result[6]
        a.enriched = result[8]
        a.http_response_data = result[10]
        a.http_response_code = result[11]
        return a

    def _create_car_from_db_result(self, result) -> Car:
        """Takes a tuple and returns an instance of Car"""
        car = Car(title=result[3])
        car.hash = result[0]
        car.short_hash = result[1]
        car.link = result[2]
        car.model = result[4]
        car.price = result[5]
        car.year = result[6]
        car.mileage = result[7]
        car.engine = result[8]
        car.first_seen = arrow.get(result[9])
        car.last_seen = arrow.get(result[10])
        car.enriched_time = arrow.get(result[11])
        car.gearbox = result[12]
        car.color = result[13]
        car.inspection = result[14]
        car.description = result[15]
        car.enriched = result[16]
        car.published = arrow.get(result[17])
        car.http_response_data = result[18]
        car.http_response_code = result[19]
        return car

    def _get_all_cars(self) -> list[Car]:
        self.cur.execute("select * from cars order by published desc")
        results = self.cur.fetchall()
        cars_list = []
        for r in results:
            cars_list.append(self._create_car_from_db_result(r))
        return cars_list

    def _get_all_houses(self) -> list[House]:
        self.cur.execute("select * from houses order by published desc")
        results = self.cur.fetchall()
        houses_list = []
        for r in results:
            houses_list.append(self._create_house_from_db_result(r))
        return houses_list

    def _get_all_apartments(self) -> list[Apartment]:
        self.cur.execute("select * from apartments order by published desc")
        results = self.cur.fetchall()
        apartments_list = []
        for r in results:
            apartments_list.append(self._create_apartment_from_db_result(r))
        return apartments_list

    def _classified_house_exists(self, classified: House) -> bool:
        self.cur.execute(
            "select count(*) from houses where hash = '%s'" % classified.hash
        )
        results = self.cur.fetchall()
        if len(results) == 0:
            return False
        elif len(results) == 1:
            result = results[0]
            if type(result) == tuple:
                if result[0] == 0:
                    return False
                else:
                    return True
            else:
                raise Exception("Unexpected result type encountered")
        else:
            raise Exception("Unexpected number of database results returned")

    def _classified_apartment_exists(self, classified: Apartment) -> bool:
        self.cur.execute(
            "select count(*) from apartments where hash = '%s'" % classified.hash
        )
        results = self.cur.fetchall()
        if len(results) == 0:
            return False
        elif len(results) == 1:
            result = results[0]
            if type(result) == tuple:
                if result[0] == 0:
                    return False
                else:
                    return True
            else:
                raise Exception("Unexpected result type encountered")
        else:
            raise Exception("Unexpected number of database results returned")

    def _classified_car_exists(self, classified: Car) -> bool:
        self.cur.execute(
            "select count(*) from cars where hash = '%s'" % classified.hash
        )
        results = self.cur.fetchall()
        if len(results) == 0:
            return False
        elif len(results) == 1:
            result = results[0]
            if type(result) == tuple:
                if result[0] == 0:
                    return False
                else:
                    return True
            else:
                raise Exception("Unexpected result type encountered")
        else:
            raise Exception("Unexpected number of database results returned")

    def classified_exists(self, classified: Classified) -> bool:
        if classified.category == "apartment":
            return self._classified_apartment_exists(classified)
        elif classified.category == "house":
            return self._classified_house_exists(classified)
        elif classified.category == "car":
            return self._classified_car_exists(classified)
        else:
            raise NotImplementedError()

    def get_all_classifieds(self, category="*") -> list:
        """Returns a list of all classifieds."""
        if not self._is_valid_category(category):
            raise ValueError("Invalid category specified")
        if category == "apartment":
            return self._get_all_apartments()
        elif category == "house":
            return self._get_all_houses()
        elif category == "car":
            return self._get_all_cars()
        elif category == "*":
            all_classifieds = []
            all_classifieds.extend(self._get_all_apartments())
            all_classifieds.extend(self._get_all_houses())
            all_classifieds.extend(self._get_all_cars())
            return all_classifieds
        else:
            raise NotImplementedError()

    def _write_classified_apartment(self, apartment: lib.datastructures.Apartment):
        sql = """insert into apartments
                (hash, short_hash, link, title, rooms, floor,
                price, street, enriched, published, http_response_data, http_response_code)
                values (?,?,?,?,?,?,?,?,?,?,?,?)"""
        sql_data = (
            apartment.hash,
            apartment.short_hash,
            apartment.link,
            apartment.title,
            apartment.rooms,
            apartment.floor,
            apartment.price,
            apartment.street,
            apartment.enriched,
            apartment.published.datetime,
            apartment.http_response_data,
            apartment.http_response_code
        )

        self.cur.execute(sql, sql_data)
        self.con.commit()

    def _write_classified_house(self, house: lib.datastructures.House):
        sql = """insert into houses
                (hash, short_hash, link, title, rooms, floor,
                price, street, enriched, published, http_response_data, http_response_code)
                values (?,?,?,?,?,?,?,?,?,?,?,?)"""
        sql_data = (
            house.hash,
            house.short_hash,
            house.link,
            house.title,
            house.rooms,
            house.floor,
            house.price,
            house.street,
            house.enriched,
            house.published.datetime,
            house.http_response_code,
            house.http_response_data
        )

        self.cur.execute(sql, sql_data)
        self.con.commit()

    def _write_classified_car(self, car: lib.datastructures.Car):
        sql = """insert into cars
                (hash, short_hash, link, title, model, price,
                year, mileage, engine, first_seen, last_seen, enriched_time,
                gearbox, color, inspection, description,
                enriched, published, http_response_data, http_response_code)
                values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
        sql_data = (
            car.hash,
            car.short_hash,
            car.link,
            car.title,
            car.model,
            car.price,
            car.year,
            car.mileage,
            car.engine,
            car.first_seen.datetime,
            car.last_seen.datetime,
            car.enriched_time.datetime,
            car.gearbox,
            car.color,
            car.inspection,
            car.description,
            car.enriched,
            car.published.datetime,
            car.http_response_data,
            car.http_response_code
        )

        self.cur.execute(sql, sql_data)
        self.con.commit()


    def write_classified(self, classified: lib.datastructures.Classified):
        self.stats.set_last_objects_update()
        if classified.category == "apartment":
            self._write_classified_apartment(classified)
            return True
        elif classified.category == "house":
            self._write_classified_house(classified)
            return True
        elif classified.category == "car":
            self._write_classified_car(classified)
            return True
        else:
            raise ValueError("Unsupported classified category")

    def _update_classified_apartment(self, apartment: Apartment):
        sql = """update apartments set title = ?, rooms = ?, floor = ?, price = ?, street = ?, enriched = ?, published = ?, http_response_data = ?, http_response_code =? where hash = ?"""
        sql_data = (
            apartment.title,
            apartment.rooms,
            apartment.floor,
            apartment.price,
            apartment.street,
            apartment.enriched,
            apartment.published.datetime,
            apartment.http_response_data,
            apartment.http_response_code,
            apartment.hash
        )
        self.cur.execute(sql, sql_data)
        self.con.commit()

    def _update_classified_house(self, house: House):
        sql = """update houses set title = ?, rooms = ?, floor = ?, price = ?, street = ?, enriched = ?, published = ?, http_response_data = ?, http_response_code = ? where hash = ?"""
        sql_data = (
            house.title,
            house.rooms,
            house.floor,
            house.price,
            house.street,
            house.enriched,
            house.published.datetime,
            house.http_response_data,
            house.http_response_code,
            house.hash
        )
        self.cur.execute(sql, sql_data)
        self.con.commit()

    def _update_classified_car(self, car: Car):
        sql = """update cars set title = ?, model = ?, price = ?, year = ?, mileage = ?, engine = ?, first_seen = ?, last_seen = ?, enriched_time = ?, gearbox = ?, color = ?, inspection = ?, description = ?, enriched = ?, published = ?, http_response_data =?, http_response_code =? where hash = ?"""
        sql_data = (
            car.title,
            car.model,
            car.price,
            car.year,
            car.mileage,
            car.engine,
            car.first_seen.datetime,
            car.last_seen.datetime,
            car.enriched_time.datetime,
            car.gearbox,
            car.color,
            car.inspection,
            car.description,
            car.enriched,
            car.published.datetime,
            car.http_response_data,
            car.http_response_code,
            car.hash,
        )
        self.cur.execute(sql, sql_data)
        self.con.commit()

    def update_classified(self, classified: Classified):
        logger.debug(f"Updating classified {classified.title} / {classified.short_hash}")
        if classified.category == "apartment":
            return self._update_classified_apartment(classified)
        elif classified.category == "house":
            return self._update_classified_house(classified)
        elif classified.category == "car":
            return self._update_classified_car(classified)
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

    def classified_exists(self, classified: Classified) -> bool:
        # not implemented yet
        return True

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
        now = arrow.now()
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
