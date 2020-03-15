import logging
import sys
import os
import requests
from lxml import html

import lib.cache
import datetime
from lib.datastructures import Apartment, House
from lib.log import func_log


class Retriever:

    def __init__(self, settings):
        self.settings = settings
        self.data_cache = lib.cache.DataCache(self.settings)

    @func_log
    def is_cache_fresh(self):
        if not os.path.exists(self.data_cache.local_cache):
            logging.debug("Local cache {} does not exist".format(self.data_cache.local_cache))
            return False
        current_timestamp = datetime.datetime.now()
        cache_timestamp = self.data_cache.get_timestamp()
        delta = current_timestamp - cache_timestamp
        delta_seconds = delta.total_seconds()
        if delta_seconds > self.settings["cache_freshness"]:
            logging.debug("Cache is not fresh. Delta: {} seconds".format(delta_seconds))
            return False
        else:
            logging.debug("Cache is fresh. Delta: {} seconds".format(delta_seconds))
            return True

    @func_log
    def update_data_cache(self):
        tracking_list = self.settings["tracking_list"]

        for item in tracking_list:
            url = tracking_list[item]["url"]
            logging.info("Looking for type: {}".format(item))
            # TODO: only update cache if it is cold

            data = self.retrieve_ss_data(url)
            logging.debug("{} -> {}".format(url, data))
            self.data_cache.add(url, data)
    @func_log
    def retrieve_ss_data(self,url: str) -> object:
        """Retrieve SS.COM data.

        Retrieve the data using the URL provided.
        """

        # hashlib.sha256(str(self).encode("utf-8")).hexdigest()
        headers = {
            "User-Agent":
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"
        }
        r = requests.get(url, headers=headers)
        return r.content

    def get_ss_data_from_cache(self,url: str) -> object:
        logging.debug("Retrieving data from cache for URL: {}".format(url))
        data = self.data_cache.get(url)
        tree = html.fromstring(data)
        return tree.xpath("//*[@id=\"filter_frm\"]/table[2]")[0]


    def get_id_from_attrib(self,attrib):
        return attrib["id"].split("_")[1]

    def get_text_from_element(self,k, xpath):
        return k.body.findall(xpath)[0].text
    @func_log
    def find_apartment_by_id(self,k, id):

        title = self.get_text_from_element(k,
                                      ".//a[@id=\"dm_{}\"]".format(id))
        street = self.get_text_from_element(
            k, ".//*[@id=\"tr_{}\"]/td[4]".format(id))

        if title is None or street is None:
            logging.warning("Invalid data for classified with ID: {}".format(id))
            return False
        apartment = Apartment(title, street)

        apartment.rooms = self.get_text_from_element(
            k, ".//*[@id=\"tr_{}\"]/td[5]".format(id))
        apartment.space = self.get_text_from_element(
            k, ".//*[@id=\"tr_{}\"]/td[6]".format(id))
        apartment.floor = self.get_text_from_element(
            k, ".//*[@id=\"tr_{}\"]/td[7]".format(id))
        apartment.series = self.get_text_from_element(
            k, ".//*[@id=\"tr_{}\"]/td[8]".format(id))
        apartment.price_per_m = self.get_text_from_element(
            k, ".//*[@id=\"tr_{}\"]/td[9]".format(id))
        apartment.price = self.get_text_from_element(
            k, ".//*[@id=\"tr_{}\"]/td[10]".format(id))
        return apartment
    @func_log
    def find_house_by_id(self,k, id):
        house = House()
        house.title = self.get_text_from_element(k, ".//a[@id=\"dm_{}\"]".format(id))
        house.street = self.get_text_from_element(
            k, ".//*[@id=\"tr_{}\"]/td[4]".format(id))
        house.space = self.get_text_from_element(k,
                                            ".//*[@id=\"tr_{}\"]/td[5]".format(id))
        house.floors = self.get_text_from_element(
            k, ".//*[@id=\"tr_{}\"]/td[6]".format(id))
        house.rooms = self.get_text_from_element(k,
                                            ".//*[@id=\"tr_{}\"]/td[7]".format(id))
        house.land = self.get_text_from_element(k,
                                           ".//*[@id=\"tr_{}\"]/td[8]".format(id))
        house.price = self.get_text_from_element(k,
                                            ".//*[@id=\"tr_{}\"]/td[9]".format(id))
        return house
    @func_log
    def get_ad_list(self,content, ad_type):
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
                    logging.debug("Skipping invalid apartment: {}".format(apartment))
            elif ad_type == "house":
                house = self.find_apartment_by_id(content, i)
                if house.title:
                    ad_list.append(house)
                else:
                    logging.debug("Skipping invalid house: {}".format(apartment))
            else:
                logging.warning("Unknown classified type!")
                sys.exit(1)
        return ad_list