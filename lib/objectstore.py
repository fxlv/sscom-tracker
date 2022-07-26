import datetime
import os
import pickle
import sqlite3
from pathlib import Path

import arrow
from loguru import logger

import lib.datastructures
import lib.settings
from lib.datastructures import Apartment, Car, Classified, House, Land
from lib.stats import TrackerStatsSql
from lib.store import ObjectStore


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
        self.stats = TrackerStatsSql(self.s)
        self.known_cities = {"Liepāja":(56.5142, 21.0126), "Rīga":(56.9850539, 24.200413)}
        logger.trace("ObjectStoreSqlite ready")

    def get_classified_count(self, category) -> int:
        if category == "apartment":
            return self._get_count_apartments()
        elif category == "house":
            return self._get_count_houses()
        elif category == "car":
            return self._get_count_cars()
        elif category == "dog":
            return self._get_count_dogs()
        elif category == "land":
            return self._get_count_land()
        else:
            raise ValueError("Unsupported classified category")

    def _get_count_dogs(self) -> int:
        self.cur.execute("select count(*) from dogs")
        results = self.cur.fetchall()
        if len(results) == 1:
            result = results[0]
            return result[0]
        else:
            raise Exception("Unexpected result received from DB")

    def _get_count_land(self) -> int:
        self.cur.execute("select count(*) from land")
        results = self.cur.fetchall()
        if len(results) == 1:
            result = results[0]
            return result[0]
        else:
            raise Exception("Unexpected result received from DB")

    def _get_count_cars(self) -> int:
        self.cur.execute("select count(*) from cars")
        results = self.cur.fetchall()
        if len(results) == 1:
            result = results[0]
            return result[0]
        else:
            raise Exception("Unexpected result received from DB")

    def _get_count_houses(self) -> int:
        self.cur.execute("select count(*) from houses")
        results = self.cur.fetchall()
        if len(results) == 1:
            result = results[0]
            return result[0]
        else:
            raise Exception("Unexpected result received from DB")

    def _get_count_apartments(self) -> int:
        self.cur.execute("select count(*) from apartments")
        results = self.cur.fetchall()
        if len(results) == 1:
            result = results[0]
            return result[0]
        else:
            raise Exception("Unexpected result received from DB")

    def _get_classified_house(self, hash_string: str) -> lib.datastructures.Apartment:
        self.cur.execute("select * from houses where hash = '%s'" % hash_string)
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
        self.cur.execute("select * from apartments where hash = '%s'" % hash_string)
        results = self.cur.fetchall()
        if len(results) == 0:
            return None
        elif len(results) == 1:
            result = results[0]
            return self._create_apartment_from_db_result(result)
        else:
            raise Exception("Unexpected number of database results returned")

    def _get_classified_car(self, hash_string: str) -> lib.datastructures.Car:
        self.cur.execute("select * from cars where hash = '%s'" % hash_string)
        results = self.cur.fetchall()
        if len(results) == 0:
            return None
        elif len(results) == 1:
            result = results[0]
            return self._create_car_from_db_result(result)
        else:
            raise Exception("Unexpected number of database results returned")

    def _get_classified_land(self, hash_string: str) -> lib.datastructures.Land:
        self.cur.execute("select * from land where hash = '%s'" % hash_string)
        results = self.cur.fetchall()
        if len(results) == 0:
            return None
        elif len(results) == 1:
            result = results[0]
            return self._create_land_from_db_result(result)
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
        elif category == "land":
            return self._get_classified_land(hash_string)
        else:
            raise ValueError("Unsupported classified category")

    def _is_valid_category(self, category_string) -> bool:
        valid_categories = ["apartment", "car", "house", "dog", "land", "*"]
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
        a.city = result[12]
        enriched_time = result[13]
        if enriched_time:
            a.enriched_time = arrow.get(enriched_time)
        else:
            a.enriched_time = None
        a.coordinates_string = result[14]
        if a.coordinates_string:
            coord_split = a.coordinates_string.split()
            if len(coord_split) ==2:
                a.coordinates = (float(coord_split[0]), float(coord_split[1]))
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
        car.price_int = result[20]
        car.mileage_int = result[21]
        return car
    
    def _create_land_from_db_result(self, result) -> Land:
        """Takes a tuple and returns an instance of Land"""
        land = Land(title=result[3], link=result[2])
        land.hash = result[0]
        land.short_hash = result[1]
        land.link = result[2]
        land.district = result[6]
        land.price = result[4]
        land.area = result[5]
        land.parish = result[7]
        land.village = result[8]
        land.street = result[9]
        land.description = result[10]
        land.cadastre = result[11]
        land.enriched = result[12]
        land.published = arrow.get(result[13])
        land.http_response_data = result[14]
        land.http_response_code = result[15]
        return land

    def _get_cars(self, limit=None, order_by=None) ->list[Car]:

        sql_query = "select * from cars"

        if order_by and order_by == "mileage":
            sql_query += " order by mileage_int asc, published desc"
        elif order_by and order_by == "price":
            sql_query += " order by price_int asc, published desc"
        else:
            sql_query +=  " order by published desc"

        if limit:
            sql_query += f" limit {limit}"

        self.cur.execute(sql_query)
        results = self.cur.fetchall()
        cars_list = []
        for r in results:
            cars_list.append(self._create_car_from_db_result(r))
        return cars_list

    def _get_all_cars(self, order_by=None) -> list[Car]:
        return self._get_cars(limit=None, order_by=order_by)

    def _get_latest_cars(self, order_by=None) -> list[Car]:
        return self._get_cars(limit=100, order_by=order_by)

    def _get_latest_houses(self, order_by=None) -> list[House]:
        return self._get_houses(limit=100, order_by=None)
    def _get_all_houses(self, order_by) -> list[House]:
        return self._get_houses(limit=None, order_by=None)
    def _get_houses(self, limit=None, order_by=None) -> list[House]:
        if limit:
            self.cur.execute(f"select * from houses order by published desc limit {limit}")
        else:
            self.cur.execute("select * from houses order by published desc")
        results = self.cur.fetchall()
        houses_list = []
        for r in results:
            houses_list.append(self._create_house_from_db_result(r))
        return houses_list

    def _get_all_apartments(self, order_by=None) -> list[Apartment]:
        return self._get_apartments(limit=None, order_by=None)
    def _get_latest_apartments(self, order_by = None, city = None) -> list[Apartment]:
        return self._get_apartments(limit=100, order_by=order_by, city = city)
    


    def get_cities(self) -> list:
        return self.known_cities.keys()

    def is_a_known_city(self, city):
        return city in self.get_cities()

    def get_city_coordinates(self,city):
        if self.is_a_known_city(city):
            return self.known_cities[city]
        else:
            return None

    def _get_apartments(self, limit=None, order_by=None, city=None) -> list[Apartment]:
        sql = "select * from apartments"
        if city:
            if self.is_a_known_city(city):
                sql += f" where city = '{city}'"
            else:
                logger.warning(f"Unknow city '{{city}}' requested")

        sql += " order by published desc"
        if limit:
            sql += f" limit {limit}"
        logger.debug(f"Executing SQL: {sql}")
        self.cur.execute(sql)
        results = self.cur.fetchall()
        apartments_list = []
        for r in results:
            apartments_list.append(self._create_apartment_from_db_result(r))
        return apartments_list
    
    # land

    def _get_land(self, limit=None, order_by=None) -> list[Land]:
        if limit:
            self.cur.execute(f"select * from land order by published desc limit {limit}")
        else:
            self.cur.execute(f"select * from land order by published desc")
        results = self.cur.fetchall()
        apartments_list = []
        for r in results:
            apartments_list.append(self._create_land_from_db_result(r))
        return apartments_list
    
    def _get_all_land(self, order_by=None) -> list[Land]:
        return self._get_land(limit=None, order_by=order_by)
    def _get_latest_land(self, order_by=None) -> list[Land]:
        return self._get_land(limit=100, order_by=order_by)

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

    def _classified_land_exists(self, classified: Land) -> bool:
        self.cur.execute(
            "select count(*) from  land where hash = '%s'" % classified.hash
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
        elif classified.category == "land":
            return self._classified_land_exists(classified)
        else:
            raise NotImplementedError()

    def get_latest_classifieds(self, category="*", order_by=None) -> list:
        """Returns a list of all classifieds."""
        if not self._is_valid_category(category):
            raise ValueError("Invalid category specified")
        if category == "apartment":
            return self._get_latest_apartments(order_by)
        elif category == "house":
            return self._get_latest_houses(order_by)
        elif category == "car":
            return self._get_latest_cars(order_by)
        elif category == "land":
            return self._get_latest_land(order_by)
        elif category == "*":
            all_classifieds = []
            all_classifieds.extend(self._get_latest_apartments(order_by))
            all_classifieds.extend(self._get_latest_houses(order_by))
            all_classifieds.extend(self._get_latest_cars(order_by))
            all_classifieds.extend(self._get_latest_land(order_by))
            return all_classifieds
        else:
            raise NotImplementedError()

    def get_all_classifieds(self, category="*", order_by = None) -> list:
        """Returns a list of all classifieds."""
        if not self._is_valid_category(category):
            raise ValueError("Invalid category specified")
        if category == "apartment":
            return self._get_all_apartments(order_by)
        elif category == "house":
            return self._get_all_houses(order_by)
        elif category == "car":
            return self._get_all_cars(order_by)
        elif category == "land":
            return self._get_all_land(order_by)
        elif category == "*":
            all_classifieds = []
            all_classifieds.extend(self._get_all_apartments(order_by))
            all_classifieds.extend(self._get_all_houses(order_by))
            all_classifieds.extend(self._get_all_cars(order_by))
            all_classifieds.extend(self._get_all_land(order_by))
            return all_classifieds
        else:
            raise NotImplementedError()

    def _write_classified_apartment(self, apartment: lib.datastructures.Apartment):
        sql = """insert into apartments
                (hash, short_hash, link, title, rooms, floor,
                price, street, enriched, published, http_response_data, http_response_code, city, enriched_time, coordinates)
                values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
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
            apartment.http_response_code,
            apartment.city,
            apartment.enriched_time,
            apartment.coordinates_string
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
            house.http_response_data,
        )

        self.cur.execute(sql, sql_data)
        self.con.commit()

    def _write_classified_car(self, car: lib.datastructures.Car):
        sql = """insert into cars
                (hash, short_hash, link, title, model, price,
                year, mileage, engine, first_seen, last_seen, enriched_time,
                gearbox, color, inspection, description,
                enriched, published, http_response_data, http_response_code, price_int, mileage_int)
                values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
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
            car.http_response_code,
            car.price_int,
            car.mileage_int
        )

        self.cur.execute(sql, sql_data)
        self.con.commit()
    
    def _write_classified_land(self, land: lib.datastructures.Land):
        sql = """insert into land 
                (hash, short_hash, link, title, price,
                area, district, parish, village, street, description,
                cadastre, enriched, published, http_response_data, http_response_code)
                values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
        sql_data = (
            land.hash,
            land.short_hash,
            land.link,
            land.title,
            land.price,
            land.area,
            land.district,
            land.parish,
            land.village,
            land.street,
            land.description,
            land.cadastre,
            land.enriched,
            land.published.datetime,
            land.http_response_data,
            land.http_response_code,
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
        elif classified.category == "land":
            self._write_classified_land(classified)
            return True
        else:
            raise ValueError("Unsupported classified category")

    def _update_classified_apartment(self, apartment: Apartment):
        sql = """update apartments set title = ?, rooms = ?, floor = ?, price = ?, street = ?, enriched = ?, published = ?, http_response_data = ?, http_response_code =?, city = ?, enriched_time = ?, coordinates = ? where hash = ?"""
        enriched_time = apartment.enriched_time
        if enriched_time:
            enriched_time = enriched_time.datetime
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
            apartment.city,
            enriched_time,
            apartment.coordinates_string,
            apartment.hash,
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
            house.hash,
        )
        self.cur.execute(sql, sql_data)
        self.con.commit()

    def _update_classified_car(self, car: Car):
        sql = """update cars set title = ?, model = ?, price = ?, year = ?, mileage = ?, engine = ?, first_seen = ?, last_seen = ?, enriched_time = ?, gearbox = ?, color = ?, inspection = ?, description = ?, enriched = ?, published = ?, http_response_data =?, http_response_code =?, price_int = ?, mileage_int = ? where hash = ?"""
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
            car.price_int,
            car.mileage_int,
            car.hash
        )
        self.cur.execute(sql, sql_data)
        self.con.commit()
    
    
    def _update_classified_land(self, land: lib.datastructures.Land):
        sql = """update land set link = ?, title = ?, price = ?,
                area = ?, district = ?, parish = ?, village = ?, street = ?, description = ?,
                cadastre = ?, enriched = ?, published = ? where hash = ?"""
        sql_data = (
            land.link,
            land.title,
            land.price,
            land.area,
            land.district,
            land.parish,
            land.village,
            land.street,
            land.description,
            land.cadastre,
            land.enriched,
            land.published.datetime,
            land.hash
        )
        self.cur.execute(sql, sql_data)
        self.con.commit()

    def update_classified(self, classified: Classified):
        logger.debug(
            f"Updating classified {classified.title} / {classified.short_hash}"
        )
        if classified.category == "apartment":
            return self._update_classified_apartment(classified)
        elif classified.category == "house":
            return self._update_classified_house(classified)
        elif classified.category == "car":
            return self._update_classified_car(classified)
        elif classified.category == "land":
            return self._update_classified_land(classified)
        else:
            raise NotImplementedError()


class ObjectStoreFiles(ObjectStore):
    def __init__(self, settings: lib.settings.Settings):
        self.s = settings
        self.object_cache_dir = self.s.object_cache_dir
        self._create_cache_dir_if_not_exists()
        self.stats = TrackerStatsSql(self.s)
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
        self.stats.set_last_objects_update()
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
