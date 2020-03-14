import lib.datastructures


def test_classified_get_hash():
    c = lib.datastructures.Classified("Something","Some street")
    c.id = 1
    assert c.get_hash() == "95e2b9327793f1781bfb212047301a992e8e7b0b9b4c38ede46a8044a96c6d25"

def test_classified_str():
    c = lib.datastructures.Classified("Something","Some street")
    c.id = 1
    assert str(c) == "Classified: Something / Str: Some street"

def test_classified_repr():
    c = lib.datastructures.Classified("Something","Some street")
    c.id = 1
    assert repr(c) == "Classified(\"Something\",\"Some street\")"

def test_classified_hash():
    c = lib.datastructures.Classified("Something", "Some street")
    assert c.__hash__() == hash((c.title, c.street))

def test_classified_eq():
    c = lib.datastructures.Classified("Something", "Some street")
    c2 = lib.datastructures.Classified("Something", "Some street")
    assert c == c2


def test_apartment_str():
    c = lib.datastructures.Apartment("Something","Some street")
    c.id = 1
    c.rooms = 2
    c.floor = 1
    assert str(c) == "Apartment: Something / Str: Some street / rooms: 2 / floor: 1"

def test_home_str():
    c = lib.datastructures.House("Something","Some street")
    c.id = 1
    assert str(c) == "House: Something / Str: Some street"