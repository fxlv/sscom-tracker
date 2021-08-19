import hashlib

from loguru import logger
from lib.log import normalize


class Classified:
    """Base class for all classifieds"""

    category: str = "uncategorized"

    def __init__(self, title, street):
        """Construct a classified object.

        Title and street are mandatory.
        As the hash of the object will be built using them.
        """
        self.title = title.strip()
        # sometimes there are newlines in the title, get rid of them
        self.title = self.title.replace("\r\n", "").replace("\n", "")
        self.street = street
        if self.street is None:
            self.street = "undetermined"
        self.street = self.street.strip()
        self.hash = self.get_hash()
        self.short_hash = self.hash[:10] #useful in logs

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        # normalize title, by trying to remove any Unicode and strip to first 10 characters
        title = normalize(self.title)
        title = title[:10]
        repr = f'Classified("{title}"")'
        return repr

    def __hash__(self):
        logger.trace("Calling built in hash method")
        return hash((self.title, self.street))

    def __eq__(self, other):
        logger.trace("Calling built in __eq__ method")
        return self.hash == other.hash

    def get_hash(self) -> str:
        """Return hash based on title and street."""
        return hashlib.sha256(str(self.title + self.street).encode("utf-8")).hexdigest()


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


class Apartment(Classified):
    category = "apartment"

    def __str__(self):
        return "Apartment: {} / Str: {} / rooms: {} / floor: {}".format(
            self.title, self.street, self.rooms, self.floor
        )


class House(Classified):
    category = "house"
    def __str__(self):
        return f"House: {self.title} / Str: {self.street}"


class Dog(Animal):
    def __str__(self):
        return f"Dog: {self.title} / Age: {self.age}"
