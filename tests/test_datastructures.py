import sys
import os
sys.path.append(os.path.dirname(os.getcwd()))
print(sys.path)
import lib.datastructures


def test_classified_get_hash():
    c = lib.datastructures.Classified()
    c.title = "Something"
    c.id = 1
    c.street = "Some street"
    assert c.get_hash() == "45cd946f4b8639863515c457231f6ba3f82c96a70fd31b9d134a4ef14f5a3ab3"

def test_classified_str():
    c = lib.datastructures.Classified()
    c.title = "Something"
    c.id = 1
    c.street = "Some street"
    assert str(c) == "Classified: Something / Str: Some street"

def test_classified_repr():
    c = lib.datastructures.Classified()
    c.title = "Something"
    c.id = 1
    c.street = "Some street"
    assert str(c) == "Classified: Something / Str: Some street"

def test_apartment_str():
    c = lib.datastructures.Apartment()
    c.title = "Something"
    c.id = 1
    c.street = "Some street"
    c.rooms = 2
    c.floor = 1
    assert str(c) == "Apartment: Something / Str: Some street / rooms: 2 / floor: 1"

def test_home_str():
    c = lib.datastructures.House()
    c.title = "Something"
    c.id = 1
    c.street = "Some street"
    assert str(c) == "House: Something / Str: Some street"