import datetime
import hashlib
import logging
import os
import pickle
import random
import sys
from abc import ABC, abstractmethod
from pathlib import Path

import arrow
import feedparser
import requests
import re
from loguru import logger
from lxml import html

import lib.settings
from lib.datastructures import Apartment, House, Dog
from lib.log import func_log
from bs4 import BeautifulSoup
from lib.helpers import strip_sscom, hash, shorthash

class Store(ABC):
    """Abstract class that defines the interface for storage."""

    @abstractmethod
    def write(self, data):
        pass


class ObjectParser:
    """Parse the data and return it as one of the supported data structures."""

    def __init__(self):
        self.supported_categories = ["apartment", "house", "car"]

    def _get_apartment_from_rss(self, rss_entry) -> lib.datastructures.Apartment:
        summary = rss_entry.summary
        soup = BeautifulSoup(summary, "html.parser")
        soup.a.extract()  # remove the first link as we don't use it
        soup.a.extract()  # remove the second link
        # now, strip all the hmtml and use regex to extract details from the remaining text
        text = soup.text.strip()
        street = re.findall("Iela: (.+)Ist.:", text)[0]
        rooms = re.findall("Ist.: (.+)m2", text)[0]
        floor = re.findall("vs: (.+)Sērija", text)[0]
        m2 = re.findall("m2: (.+)St", text)[0]
        price = re.findall("Cena: (.+) ", text)[0].strip()
        title = rss_entry.title
        apartment = lib.datastructures.Apartment(title)
        apartment.street = street
        apartment.floor = floor
        apartment.rooms = rooms
        apartment.m2 = m2
        apartment.price = price
        apartment.published = arrow.get(rss_entry["published_parsed"])
        apartment.done()
        return apartment

    def _try_get(self, regex, string, warn=True):
        """Try to extract value based on regex.

        Returns: either the extracted value or None
        """
        try:
            return re.findall(regex,string)[0]
        except IndexError:
            if warn:
                logger.trace(f"Could not extract value from object.Regex: {lib.log.normalize(regex)}, object: {lib.log.normalize(string)}")
            return None

    def _get_car_from_rss(self, rss_entry) -> lib.datastructures.House:
        summary = rss_entry.summary
        soup = BeautifulSoup(summary, "html.parser")
        soup.a.extract()  # remove the first link as we don't use it
        soup.a.extract()  # remove the second link
        # now, strip all the hmtml and use regex to extract details from the remaining text
        text = soup.text.strip()
        model = self._try_get("Modelis: (.+)Gads:", text)
        mileage = self._try_get("Nobrauk.: (.+)tūkst.", text)
        price = self._try_get("Cena: (.+)  ", text)
        year = self._try_get("Gads: (.+)Tilp", text)
        title = rss_entry.title
        car = lib.datastructures.Car(title)
        car.mileage = mileage
        car.year = year
        car.price = price
        car.model = model
        car.published = arrow.get(rss_entry["published_parsed"])
        car.done()
        return car
    def _get_house_from_rss(self, rss_entry) -> lib.datastructures.House:
        summary = rss_entry.summary
        soup = BeautifulSoup(summary, "html.parser")
        soup.a.extract()  # remove the first link as we don't use it
        soup.a.extract()  # remove the second link
        # now, strip all the hmtml and use regex to extract details from the remaining text
        text = soup.text.strip()
        street = self._try_get("Iela: (.+)m2:", text)
        m2 = self._try_get("m2: (.+)Stāvi:", text)
        floors = self._try_get("Stāvi: (.+)Ist", text)
        if not floors:
            # try a different regex, used for non-Riga houses
            floors = self._try_get("Stāvi: (.+)Zem", text)

        rooms = self._try_get("Ist.: (.+)Zem", text)
        land_m2 = self._try_get("Zem. pl.: (.+) m", text, warn=False)
        land_ha = self._try_get("Zem. pl.: (.+) ha", text, warn=False)
        price = self._try_get("Cena: (.+)  ", text)
        title = rss_entry.title
        house = lib.datastructures.House(title)
        house.street = street
        house.floors = floors
        house.rooms = rooms
        house.m2 = m2
        house.land_m2 = land_m2
        house.price = price
        house.published = arrow.get(rss_entry["published_parsed"])
        house.done()
        return house

    def _parser_factory(self, category):
        if category == "apartment":
            return self._get_apartment_from_rss
        elif category == "house":
            return self._get_house_from_rss
        elif category == "car":
            return self._get_car_from_rss

    def parse_object(self, rss_object):
        retrieval_date = rss_object.retrieved_time.strftime("%d.%m.%y")
        retrieval_time = rss_object.retrieved_time.strftime("%H:%M:%S")
        logger.debug(f"[{rss_object.url_hash[:10]}] Parsing RSS object ({rss_object.object_category}), retrieved on {retrieval_date} at {retrieval_time} with {len(rss_object.entries)} entries")
        parsed_list = []
        if not "object_category" in rss_object.keys():
            logger.warning(f"RSS Object {rss_object} does not contain 'category'")
            return False
        if rss_object["object_category"] in self.supported_categories:
            # each RSS Store object will be a dictionary and will contain bunch of entries,
            # which are the actual "classifieds"

            entries = rss_object["entries"]
            for entry in entries:
                parser = self._parser_factory(rss_object["object_category"])
                classified_item = parser(entry)
                classified_item.retrieved = rss_object["retrieved_time"]
                classified_item.url_hash = rss_object["url_hash"]
                parsed_list.append(classified_item)
        return parsed_list


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
    def load_all(self, category="*"):
        all_files = Path(self.s.object_cache_dir).glob(f"{category}/*.classified")
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


class RSSStore(Store):
    def __init__(self, settings: lib.settings.Settings):
        self.s = settings
        self.cache_dir = self.s.cache_dir
        self._create_cache_dir_if_not_exists()

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
            logger.debug(f"[{shorthash(url)}] Cache file not present for {url}")
            return False

    @func_log
    def write(self, url, data):
        # check the settings to determine write path
        full_path = self._get_full_file_name(url)
        self._create_dir_if_not_exists(full_path)
        file_handle = full_path.open(mode="wb")
        pickle.dump(data, file_handle)
        file_handle.close()

    @func_log
    def _file_is_not_empty(self, file_name: Path):
        return file_name.stat().st_size > 0

    @func_log
    def load_all(self):
        all_files = Path(self.s.cache_dir).glob("*/*/*/*/*.rss")
        all_files_unpickled = []
        for file_name in all_files:
            if self._file_is_not_empty(file_name):
                logger.debug(f"Opening file {file_name} for binary reading")
                object = pickle.load(file_name.open(mode="rb"))
                all_files_unpickled.append(object)
        file_count = len(all_files_unpickled)
        logger.debug(f"{file_count} RSS files loaded")
        return all_files_unpickled


class RetrieverManager:
    def __init__(self, settings: lib.settings.Settings):
        """Iterate over tracking list and retrieve data using appropriate retriever."""
        self.rss = RSSRetriever()
        self.rss_store = RSSStore(settings)
        self.s = settings
        self.hashfunc = self.rss_store._hash

    def update_all(self):
        now = datetime.datetime.now()
        for category in self.s.tracking_list:
            category_items = self.s.tracking_list[category]
            for item in category_items:
                item_hash = lib.helpers.shorthash(item["url"])
                item_url = item["url"]
                logger.debug(f"[{item_hash}] Updating {category} -> ({strip_sscom(item_url)})")
                if (
                    item["type"] == "rss"
                ):  # the only known type at the moment, ignore everything else
                    # check with storage, if we have a fresh item cached for this url
                    if self.rss_store.fresh_cache_present(item_url):
                        logger.debug(f"[{item_hash}] Fresh cache present for {strip_sscom(item_url)}")
                    else:
                        # retrieve from RSS and write to storage
                        logger.debug(f"[{item_hash}] No cache available for {strip_sscom(item_url)}")
                        fresh_data = self.rss.get(item_url)
                        fresh_data["url_hash"] = self.rss_store._hash(item_url)
                        fresh_data["object_category"] = category
                        fresh_data["retrieved_time"] = now

                        self.rss_store.write(item["url"], fresh_data)
                else:
                    logger.debug(f"[{item_hash}] unsupported retrieval method")


class Retriever(ABC):
    """
    The Retriever interface.

    Describes base functionality that should be implemented by all Retrievers.
    """

    def __init__(self):
        pass

    @abstractmethod
    def get(self, url: str) -> object:
        """Retrieve the data and return a raw data object."""
        pass


class RSSRetriever(Retriever):
    @func_log
    def _fetch(self, url) -> feedparser.FeedParserDict:
        agents = []
        agents.append(
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
        )
        agents.append(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
        )
        agents.append(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36"
        )
        feedparser.USER_AGENT = random.choice(agents)
        response = feedparser.parse(url)
        logger.debug(f"[{lib.helpers.shorthash(url)}] Got response {response.status} with {len(response.entries)} entries from {response.href}")
        return response

    @func_log
    def get(self, url) -> feedparser.FeedParserDict:
        return self._fetch(url)


class HttpRetrieverOLD:
    """This used to be the main retriever, but I think it would be a good idea to just throw it out."""

    def __init__(self, settings: lib.settings.Settings, data_cache):
        self.settings = settings
        self.data_cache = data_cache

    @func_log
    def update_data_cache(self):
        tracking_list = self.settings.tracking_list

        for item in tracking_list:
            url = tracking_list[item]["url"]
            logger.info(f"Looking for type: {item}")
            # TODO: only update cache if it is cold

            data = self.retrieve_ss_data(url)
            logger.debug(f"{url} -> {data}")
            self.data_cache.add(url, data)

    @func_log
    def retrieve_ss_data(self, url: str) -> object:
        """Retrieve SS.COM data.

        Retrieve the data using the URL provided.
        """

        # TODO: add randomization of user agent here
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"
        }
        r = requests.get(url, headers=headers)
        return r.content

    def get_ss_data_from_cache(self, url: str) -> object:
        logger.debug(f"[{lib.helpers.shorthash(url)}] Retrieving data from cache for URL: {url}")
        data = self.data_cache.get(url)
        tree = html.fromstring(data)
        return tree.xpath('//*[@id="filter_frm"]/table[2]')[0]

    def get_id_from_attrib(self, attrib):
        return attrib["id"].split("_")[1]

    def get_text_from_element(self, k, xpath):
        if k.body.findall(xpath)[0].text is None:
            # Handle cases when classified has been enclosed in a <b>tag</b>
            xpath = xpath + "/b"
        return k.body.findall(xpath)[0].text

    @func_log
    def find_apartment_by_id(self, k, apartment_id):

        title = self.get_text_from_element(k, f'.//a[@id="dm_{apartment_id}"]')
        street = self.get_text_from_element(k, f'.//*[@id="tr_{apartment_id}"]/td[4]')

        if title is None or street is None:
            logger.warning(f"Invalid data for classified with ID: {apartment_id}")
            return False
        apartment = Apartment(title, street)

        apartment.rooms = self.get_text_from_element(
            k, f'.//*[@id="tr_{apartment_id}"]/td[5]'
        )
        apartment.space = self.get_text_from_element(
            k, f'.//*[@id="tr_{apartment_id}"]/td[6]'
        )
        apartment.floor = self.get_text_from_element(
            k, f'.//*[@id="tr_{apartment_id}"]/td[7]'
        )
        apartment.series = self.get_text_from_element(
            k, f'.//*[@id="tr_{apartment_id}"]/td[8]'
        )
        apartment.price_per_m = self.get_text_from_element(
            k, f'.//*[@id="tr_{apartment_id}"]/td[9]'
        )
        apartment.price = self.get_text_from_element(
            k, f'.//*[@id="tr_{apartment_id}"]/td[10]'
        )
        return apartment

    @func_log
    def find_house_by_id(self, k, house_id):

        title = self.get_text_from_element(k, f'.//a[@id="dm_{house_id}"]')
        street = self.get_text_from_element(k, f'.//*[@id="tr_{house_id}"]/td[4]')
        house = House(title, street)
        house.space = self.get_text_from_element(k, f'.//*[@id="tr_{house_id}"]/td[5]')
        house.floors = self.get_text_from_element(k, f'.//*[@id="tr_{house_id}"]/td[6]')
        house.rooms = self.get_text_from_element(k, f'.//*[@id="tr_{house_id}"]/td[7]')
        house.land = self.get_text_from_element(k, f'.//*[@id="tr_{house_id}"]/td[8]')
        house.price = self.get_text_from_element(k, f'.//*[@id="tr_{house_id}"]/td[9]')
        return house

    @func_log
    def find_dog_by_id(self, k, dog_id):
        title = self.get_text_from_element(k, f'.//a[@id="dm_{dog_id}"]')
        age = self.get_text_from_element(k, f'.//*[@id="tr_{dog_id}"]/td[4]')
        if title is None or age is None:
            logger.warning(f"Invalid data for dog with ID: {dog_id}")
            return False
        dog = Dog(title, age)
        dog.price = self.get_text_from_element(k, f'.//*[@id="tr_{dog_id}"]/td[5]')
        return dog

    @func_log
    def get_ad_list(self, content, ad_type):
        rows = content.body.findall(".//tr")[0]
        ad_rows = rows.findall("..//td[@class='msg2']/div[@class='d1']/a")

        id_list = []

        for row in ad_rows:
            id_list.append(self.get_id_from_attrib(row.attrib))

        ad_list = []
        for i in id_list:
            if ad_type == "apartment":
                apartment = self.find_apartment_by_id(content, i)
                if not apartment:
                    continue  # skip items that are False (could happen with malformed input)
                if apartment.rooms and apartment.floor:
                    ad_list.append(apartment)
                else:
                    logger.debug(f"Skipping invalid apartment: {apartment}")
            elif ad_type == "house":
                house = self.find_house_by_id(content, i)
                if house.title:
                    ad_list.append(house)
                else:
                    logger.debug(f"Skipping invalid house: {apartment}")
            elif ad_type == "dog":
                dog = self.find_dog_by_id(content, i)
                if dog.title:
                    ad_list.append(dog)
                else:
                    logger.debug(f"Skipping invalid dog: {dog}")
            else:
                logger.critical("Unknown classified type!")
                sys.exit(1)
        return ad_list
