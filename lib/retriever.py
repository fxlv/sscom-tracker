import datetime
import random
import sys
from abc import ABC, abstractmethod

import feedparser
import requests
from bs4 import BeautifulSoup
from loguru import logger
from lxml import html

import lib.settings
from lib.datastructures import Apartment, Dog, House, HttpResponse
from lib.helpers import strip_sscom
from lib.log import func_log
from lib.rssstore import RSSStore


class RetrieverManager:
    def __init__(self, settings: lib.settings.Settings):
        """Iterate over tracking list and retrieve data using appropriate retriever."""
        self.rss = RSSRetriever()
        self.rss_store = RSSStore(settings)
        self.s = settings
        self.hashfunc = self.rss_store._hash
        lib.log.set_up_logging(settings, debug=True)
        self.logger = logger.bind(task="RetrieverManager")

    def update_all(self, update_category):
        now = datetime.datetime.now()
        for category in self.s.tracking_list:
            if not update_category == "*" and not update_category == category:
                logger.debug(
                    f"Skipping update for category {category}, update category filter is set to {update_category}"
                )
                continue
            category_items = self.s.tracking_list[category]
            for item in category_items:
                item_hash = lib.helpers.shorthash(item["url"])
                item_url = item["url"]
                logger.debug(
                    f"[{item_hash}] Updating {category} -> ({strip_sscom(item_url)})"
                )
                if (
                    item["type"] == "rss"
                ):  # the only known type at the moment, ignore everything else
                    # check with storage, if we have a fresh item cached for this url
                    if self.rss_store.fresh_cache_present(item_url):
                        logger.debug(
                            f"[{item_hash}] Fresh cache present for {strip_sscom(item_url)}"
                        )
                    else:
                        # retrieve from RSS and write to storage
                        logger.debug(
                            f"[{item_hash}] No cache available for {strip_sscom(item_url)}"
                        )
                        fresh_data = self.rss.get(item_url)
                        fresh_data["url_hash"] = self.rss_store._hash(item_url)
                        fresh_data["object_category"] = category
                        fresh_data["retrieved_time"] = now

                        self.rss_store.write_classified(item["url"], fresh_data)
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
        logger.debug(
            f"[{lib.helpers.shorthash(url)}] Got response {response.status} with {len(response.entries)} entries from {response.href}"
        )
        return response

    @func_log
    def get(self, url) -> feedparser.FeedParserDict:
        return self._fetch(url)


class HttpRetriever:
    def __init__(self):
        self.l = logger

    def get_content(self, soup):
        return soup.table.table.text

    def retrieve_ss_data(self, url: str) -> HttpResponse:
        """Retrieve SS.COM data.

        Retrieve the data using the URL provided.
        """

        # TODO: add randomization of user agent here
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"
        }
        self.l.trace(f"Making a request to retrieve {url}")
        r = requests.get(url, headers=headers)
        response_code = r.status_code
        response_raw = r.content
        response_soup = BeautifulSoup(response_raw, "html.parser")
        response_content = self.get_content(response_soup)
        response_size = sys.getsizeof(response_raw)
        self.l.trace(f"Got response code: {response_code}, size: {response_size}")
        # return a parsed and raw content version, for storage
        # soup objects cannot be pickled
        return HttpResponse(response_code, response_content, response_raw)


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
        logger.debug(
            f"[{lib.helpers.shorthash(url)}] Retrieving data from cache for URL: {url}"
        )
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
