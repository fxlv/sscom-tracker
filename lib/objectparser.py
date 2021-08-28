import re

import arrow
from bs4 import BeautifulSoup
from loguru import logger

import lib.datastructures
import lib.log

class Enricher:

    def __init__(self):
        pass

    def _enrich_car(self, car: lib.datastructures.Car):

        # first task is to find the description
        # to do that, let' s strip the junk from the beginning

        content = car.http_response_data.response_content #this is where the plain text data is
        content = content.split("Atgriezties uz sludinājumu sarakstu")[1].strip()
        try:
            # now, find where the description ends
            end_of_desc = re.findall("(Marka.+Izlaiduma gads:.+Motors)", content)[0]
        except IndexError:
            logger.debug(f"The classified is missing car details. Possibly this is not a sales classified. Skipping it.")
            return car
        description = content.split(end_of_desc)[0]
        details = content.split(description)
        details = "".join(details).strip() # merge list into a string, it could be that there are some junk whitespaces, so this join approch will ensure we only have one object to worry about.


        # now search for conrete details, like engine, make, model etc in the 'details'
        engine = re.findall("Motors:(.+)Ātr.kārba", details)[0]

        try:
            # sometimes, mileage is not specified, which makes the regex for gearbox have two options
            gearbox = re.findall("Ātr.kārba:(.+)Nobraukums", details)
            if len(gearbox) == 1:
                gearbox = gearbox[0]
            else:
                gearbox = re.findall("Ātr.kārba:(.+)Krāsa:", details)[0]
        except:
            logger.warning(f"[{{car.short_hash }}] could not identify gearbox")



        try:
            color = re.findall("Krāsa:(.+)Virsbūves tips:", details)[0].strip()
        except:
            logger.warning(f"[{car.short_hash}] Could not identify color.")
            color = "unknown"


        try:
            inspection =  re.findall("Tehniskā apskate:(.+)VIN kods", details)
            if len(inspection) ==1:
                inspection = inspection[0]
            else:
                inspection = re.findall("Tehniskā apskate:(.+)Valsts", details)[0]
        except:
            logger.warning(f"[{{car.short_hash}}] Could not determine inspection")


        car.engine = engine

        # ensure all timestamps are using Arrow
        car.first_seen = arrow.get(car.first_seen)
        car.last_seen = arrow.get(car.last_seen)
        car.enriched_time = arrow.now()

        car.gearbox = gearbox
        car.color = color
        car.inspection = inspection
        car.description = description
        car.enriched = True
        return car


    def enrich(self, classified: lib.datastructures.Classified):
        if classified.category == "car":
            classified = self._enrich_car(classified)
        else:
            logger.debug(f"[{classified.short_hash}] not supported for enrichment")
        return classified

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
        apartment.link = rss_entry["link"]
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
        try:
            soup = BeautifulSoup(summary, "html.parser")
            soup.a.extract()  # remove the first link as we don't use it
            soup.a.extract()  # remove the second link
        except Exception as e:
            logger.warning(f"Exception encountered: {e}") 
            return None
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
        car.link = rss_entry["link"]
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
        house.link = rss_entry["link"]
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
                if classified_item is None:
                    logger.warning("Could not parse item, skipping.")
                    continue
                classified_item.retrieved = rss_object["retrieved_time"]
                classified_item.url_hash = rss_object["url_hash"]
                parsed_list.append(classified_item)
        return parsed_list
