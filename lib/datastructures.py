import hashlib

import arrow
from bs4 import BeautifulSoup
from loguru import logger

from lib.log import normalize


class HttpResponse:
    def __init__(self, response_code: int, response_content: str, response_raw: str):
        self.response_code = response_code
        self.response_content = response_content
        self.response_raw = response_raw


class Classified:
    """Base class for all classifieds"""

    category: str = "uncategorized"

    def __init__(self, title):
        """Construct a classified object.

        Title and street are mandatory.
        As the hash of the object will be built using them.
        """
        self.title = title.strip()
        # sometimes there are newlines in the title, get rid of them
        self.title = self.title.replace("\r\n", "").replace("\n", "")
        self.enriched = False
        self.published = None
        self.http_response_data = None
        self.http_response_code = None

    def done(self):
        self._prepare()  # this method can and should be overriden by child classes
        self.hash = self.get_hash()
        self.short_hash = self.hash[:10]  # useful in logs

    def _prepare(self):
        self.hash_string = self.title

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        # normalize title, by trying to remove any Unicode and strip to first 10 characters
        title = normalize(self.title)
        title = title[:10]
        repr = f"Classified: {title}"
        return repr

    def __hash__(self):
        logger.trace("Calling built in hash method")
        return hash(self.title)

    def __eq__(self, other):
        logger.trace("Calling built in __eq__ method")
        return self.__hash__() == other.hash

    def get_hash(self) -> str:
        """Return hash based on title and street."""
        return hashlib.sha256(str(self.hash_string).encode("utf-8")).hexdigest()


class Dwelling(Classified):
    """Base class for all living places."""

    category: str = "dwelling"

    def __init__(self, title, street):
        super().__init__(title)
        self.street = street
        self.rooms: int = 0
        self.floor: int = 0
        self.city: str = "Unknown"
        self.enriched_time = None
        self.coordinates = None

    def _prepare(self):
        if self.street is None:
            self.street = "undetermined"
        self.street = self.street.strip()
        self.hash_string = self.title + self.street


class Animal:
    """Base class for all animals"""

    def __init__(self, title, age):
        """Construct a classified object.

        Title and age are mandatory.
        As the hash of the object will be built using them.
        """
        self.title = title.strip()
        # sometimes there are newlines in the title, get rid of them
        self.title = self.title.replace("\r\n", "").replace("\n", "")
        self.age = age.strip()
        self.hash = self.get_hash()

    def __str__(self):
        return f"Classified: {self.title} / Age: {self.age}"

    def __repr__(self):
        repr = f'Classified("{self.title}","{self.age}")'
        return repr

    def __hash__(self):
        logger.debug("Calling built in hash method")
        return hash((self.title, self.age))

    def __eq__(self, other):
        logger.trace("Calling built in __eq__ method")
        return self.hash == other.hash

    def get_hash(self) -> str:
        """Return hash based on title and street."""
        return hashlib.sha256(str(self.title + self.age).encode("utf-8")).hexdigest()


class Land(Classified):
    category: str = "land"

    def __init__(self, title, link: str):
        super().__init__(title)
        self.link = link
        self.district = None
        self.parish = None
        self.street = None
        self.village = None
        self.area = None
        self.description = None
        self.cadastre = None

    def _prepare(self):
        self.hash_string = self.title + self.link

    def __str__(self):
        return f"Land: {self.title}"

class Car(Classified):
    category = "car"

    def __init__(self, title):
        super().__init__(title)
        self.color = None
        self.gearbox = "unknown"
        self.model = None
        self.description = None
        self.inspection = "unknown"
        self.engine = None
        self.year = None
        self.mileage = None
        self.first_seen = arrow.now()
        self.last_seen = arrow.now()
        self.enriched_time = arrow.now()
        self.price_int = None
        self.mileage_int = None

    def _prepare(self):
        self.hash_string = self.title + self.price

    def __str__(self):
        return f"Car: {self.model} / price: {self.price}"


class Apartment(Dwelling):
    category = "apartment"

    def __str__(self):
        return "Apartment: {} / Str: {} / rooms: {} / floor: {}".format(
            self.title, self.street, self.rooms, self.floor
        )


class House(Dwelling):
    category = "house"

    def __str__(self):
        return f"House: {self.title} / Str: {self.street}"


class Dog(Animal):
    def __str__(self):
        return f"Dog: {self.title} / Age: {self.age}"
