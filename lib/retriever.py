import sys
from loguru import logger
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
        logger.debug(f"Retrieving data from cache for URL: {url}")
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
