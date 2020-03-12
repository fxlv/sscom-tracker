class Classified:
    """Base class for all classifieds"""

    def __init__(self):
        self.title = None
        self.id = None
        self.street = None

    def __str__(self):
        return "Classified: {} / Str: {}".format(self.title, self.street)

    def __repr__(self):
        return self.__str__()


class Apartment(Classified):

    def __str__(self):
        return "Apartment: {} / Str: {} / rooms: {} / floor: {}".format(self.title, self.street, self.rooms, self.floor)


class House(Classified):

    def __str__(self):
        return "House: {} / Str: {}".format(self.title, self.street)