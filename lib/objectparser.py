import re

import arrow
from bs4 import BeautifulSoup
from loguru import logger

import lib.datastructures
import lib.log


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