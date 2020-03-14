import hashlib


class Classified:
    """Base class for all classifieds"""

    def __init__(self, title=None, street=None):
        self.title = title
        self.id = None
        self.street = street

    def __str__(self):
        return "Classified: {} / Str: {}".format(self.title, self.street)

    def __repr__(self):
        repr = "Classified(\"{}\",\"{}\")".format(self.title, self.street)
        return repr

    def get_hash(self):
        return hashlib.sha256(str(self).encode("utf-8")).hexdigest()


class Apartment(Classified):

    def __str__(self):
        return "Apartment: {} / Str: {} / rooms: {} / floor: {}".format(self.title, self.street, self.rooms, self.floor)


class House(Classified):

    def __str__(self):
        return "House: {} / Str: {}".format(self.title, self.street)
