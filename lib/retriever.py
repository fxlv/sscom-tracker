import logging
import sys

import requests
from lxml import html

from lib.datastructures import Apartment, House, Dog
from lib.log import func_log


class Retriever:

    def __init__(self, settings, data_cache):
        self.settings = settings
        self.data_cache = data_cache

    @func_log
    def update_data_cache(self):
        tracking_list = self.settings["tracking_list"]

        for item in tracking_list:
            url = tracking_list[item]["url"]
            logging.info("Looking for type: %s", item)
            # TODO: only update cache if it is cold

            data = self.retrieve_ss_data(url)
            logging.debug("%s -> %s", url, data)
            self.data_cache.add(url, data)

    @func_log
    def retrieve_ss_data(self, url: str) -> object:
        """Retrieve SS.COM data.

        Retrieve the data using the URL provided.
        """

        # TODO: add randomization of user agent here
        headers = {
            "User-Agent":
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"
        }
        r = requests.get(url, headers=headers)
        return r.content

    def get_ss_data_from_cache(self, url: str) -> object:
        logging.debug("Retrieving data from cache for URL: %s", url)
        data = self.data_cache.get(url)
        tree = html.fromstring(data)
        return tree.xpath("//*[@id=\"filter_frm\"]/table[2]")[0]

    def get_id_from_attrib(self, attrib):
        return attrib["id"].split("_")[1]

    def get_text_from_element(self, k, xpath):
        if k.body.findall(xpath)[0].text is None:
            # Handle cases when classified has been enclosed in a <b>tag</b>
            xpath = xpath+"/b"
        return k.body.findall(xpath)[0].text

    @func_log
    def find_apartment_by_id(self, k, apartment_id):

        title = self.get_text_from_element(k, ".//a[@id=\"dm_{}\"]".format(apartment_id))
        street = self.get_text_from_element(
            k, ".//*[@id=\"tr_{}\"]/td[4]".format(apartment_id))

        if title is None or street is None:
            logging.warning(
                "Invalid data for classified with ID: %s", apartment_id)
            return False
        apartment = Apartment(title, street)

        apartment.rooms = self.get_text_from_element(
            k, ".//*[@id=\"tr_{}\"]/td[5]".format(apartment_id))
        apartment.space = self.get_text_from_element(
            k, ".//*[@id=\"tr_{}\"]/td[6]".format(apartment_id))
        apartment.floor = self.get_text_from_element(
            k, ".//*[@id=\"tr_{}\"]/td[7]".format(apartment_id))
        apartment.series = self.get_text_from_element(
            k, ".//*[@id=\"tr_{}\"]/td[8]".format(apartment_id))
        apartment.price_per_m = self.get_text_from_element(
            k, ".//*[@id=\"tr_{}\"]/td[9]".format(apartment_id))
        apartment.price = self.get_text_from_element(
            k, ".//*[@id=\"tr_{}\"]/td[10]".format(apartment_id))
        return apartment

    @func_log
    def find_house_by_id(self, k, house_id):

        title = self.get_text_from_element(
            k, ".//a[@id=\"dm_{}\"]".format(house_id))
        street = self.get_text_from_element(
            k, ".//*[@id=\"tr_{}\"]/td[4]".format(house_id))
        house = House(title, street)
        house.space = self.get_text_from_element(
            k, ".//*[@id=\"tr_{}\"]/td[5]".format(house_id))
        house.floors = self.get_text_from_element(
            k, ".//*[@id=\"tr_{}\"]/td[6]".format(house_id))
        house.rooms = self.get_text_from_element(
            k, ".//*[@id=\"tr_{}\"]/td[7]".format(house_id))
        house.land = self.get_text_from_element(
            k, ".//*[@id=\"tr_{}\"]/td[8]".format(house_id))
        house.price = self.get_text_from_element(
            k, ".//*[@id=\"tr_{}\"]/td[9]".format(house_id))
        return house

    @func_log
    def find_dog_by_id(self, k, dog_id):
        title = self.get_text_from_element(
            k, ".//a[@id=\"dm_{}\"]".format(dog_id))
        age = self.get_text_from_element(
            k, ".//*[@id=\"tr_{}\"]/td[4]".format(dog_id))
        if title is None or age is None:
            logging.warning(
                "Invalid data for dog with ID: %s", dog_id)
            return False
        dog = Dog(title, age)
        dog.price = self.get_text_from_element(
            k, ".//*[@id=\"tr_{}\"]/td[5]".format(dog_id))
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
                    logging.debug(
                        "Skipping invalid apartment: %s", apartment)
            elif ad_type == "house":
                house = self.find_house_by_id(content, i)
                if house.title:
                    ad_list.append(house)
                else:
                    logging.debug(
                        "Skipping invalid house: %s", apartment)
            elif ad_type == "dog":
                dog = self.find_dog_by_id(content, i)
                if dog.title:
                    ad_list.append(dog)
                else:
                    logging.debug(
                        "Skipping invalid dog: %s", dog)
            else:
                logging.critical("Unknown classified type!")
                sys.exit(1)
        return ad_list
