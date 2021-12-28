import lib.datastructures


def test_classified_get_hash():
    c = lib.datastructures.Classified("Something")
    c.id = 1
    c.done()
    assert (
        c.get_hash()
        == "edcd3ea8b13071e3d13924cba9cc879c9101372a90706549b4d94e5337c2cbec"
    )


def test_classified_str():
    c = lib.datastructures.Classified("Something")
    c.id = 1
    assert str(c) == "Classified: Something"


def test_classified_repr():
    c = lib.datastructures.Classified("Something")
    c.id = 1
    assert repr(c) == "Classified: Something"


def test_classified_hash():
    c = lib.datastructures.Classified("Something")
    assert c.__hash__() == hash(c.title)


def test_classified_eq():
    c = lib.datastructures.Classified("Something").done()
    c2 = lib.datastructures.Classified("Something").done()
    assert c == c2


def test_apartment_str():
    c = lib.datastructures.Apartment("Something", "Some street")
    c.id = 1
    c.rooms = 2
    c.floor = 1
    assert str(c) == "Apartment: Something / Str: Some street / rooms: 2 / floor: 1"


def test_home_str():
    c = lib.datastructures.House("Something", "Some street")
    c.id = 1
    assert str(c) == "House: Something / Str: Some street"


def test_animal_str():
    a = lib.datastructures.Animal("Nice dog", "2 months")
    assert str(a) == "Classified: Nice dog / Age: 2 months"


def test_animal_repr():
    a = lib.datastructures.Animal("Nice dog", "2 months")
    assert repr(a) == 'Classified("Nice dog","2 months")'


def test_animal_hash():
    a = lib.datastructures.Animal("Nice dog", "2 months")
    assert a.__hash__() == hash((a.title, a.age))


def test_animal_eq():
    a = lib.datastructures.Animal("Nice dog", "some age")
    a2 = lib.datastructures.Animal("Nice dog", "some age")
    assert a == a2


def test_dog_str():
    a = lib.datastructures.Dog("Nice dog", "2 months")
    assert str(a) == "Dog: Nice dog / Age: 2 months"
