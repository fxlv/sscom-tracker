import hashlib
import logging

class Classified:
    """Base class for all classifieds"""

    def __init__(self, title, street):
        """Construct a classified object.

        Title and street are mandatory.
        As the hash of the object will be built using them.
        """
        self.title = title.strip()
        # sometimes there are newlines in the title, get rid of them
        self.title = self.title.replace("\r\n","").replace("\n","")
        self.street = street.strip()
        self.hash = self.get_hash()

    def __str__(self):
        return "Classified: {} / Str: {}".format(self.title, self.street)

    def __repr__(self):
        repr = "Classified(\"{}\",\"{}\")".format(self.title, self.street)
        return repr

    def __hash__(self):
        logging.debug("Calling built in hash method")
        return hash((self.title, self.street))

    def __eq__(self, other):
        logging.debug("Calling built in __eq__ method")
        return self.hash == other.hash

    def get_hash(self) -> str:
        """Return hash based on title and street."""
        return hashlib.sha256(str(self.title+self.street).encode("utf-8")).hexdigest()


class Apartment(Classified):

    def __str__(self):
        return "Apartment: {} / Str: {} / rooms: {} / floor: {}".format(self.title, self.street, self.rooms, self.floor)


class House(Classified):

    def __str__(self):
        return "House: {} / Str: {}".format(self.title, self.street)
