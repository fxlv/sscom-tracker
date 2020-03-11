#!/usr/bin/env python

import requests
from lxml import html
import hashlib
import os
import pickle
from pushover import Client
import json
import sys


local_cache = "cache.db"


class AdCache:

    def __init__(self):
        if not os.path.exists(local_cache):
            print("Local cache does not exist, will create a new one.")
            self.cache = []
        else:
            cache_file = open(local_cache,"rb")
            self.cache = pickle.load(cache_file)
    def __del__(self):
        print("Destructor called...")
        self.save()
    def add(self, h):
        """Store a hash in local cache"""
        return self.cache.append(h)

    def is_known(self, ad):
        """Check hash against local cahe"""
        h = get_ad_hash(ad)
        if h in self.cache:
            return True
        else:
            self.add(h)
            return False

    def save(self):
        with open(local_cache, "wb") as cache_file:
            pickle.dump(self.cache, cache_file)
            print("Cache file saved")


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



def get_id_from_attrib(attrib):
    return attrib["id"].split("_")[1]

def get_text_from_element(k, xpath):
    return k.body.findall(xpath)[0].text

def find_apartment_by_id(k,id):
    apartment = Apartment()
    apartment.title = get_text_from_element(k, ".//a[@id=\"dm_{}\"]".format(id))
    apartment.street = get_text_from_element(k, ".//*[@id=\"tr_{}\"]/td[4]".format(id))
    apartment.rooms = get_text_from_element(k, ".//*[@id=\"tr_{}\"]/td[5]".format(id))
    apartment.space = get_text_from_element(k, ".//*[@id=\"tr_{}\"]/td[6]".format(id))
    apartment.floor = get_text_from_element(k, ".//*[@id=\"tr_{}\"]/td[7]".format(id))
    apartment.series = get_text_from_element(k, ".//*[@id=\"tr_{}\"]/td[8]".format(id))
    apartment.price_per_m = get_text_from_element(k, ".//*[@id=\"tr_{}\"]/td[9]".format(id))
    apartment.price = get_text_from_element(k, ".//*[@id=\"tr_{}\"]/td[10]".format(id))
    return apartment

def find_house_by_id(k,id):
    house = House()
    house.title = get_text_from_element(k, ".//a[@id=\"dm_{}\"]".format(id))
    house.street = get_text_from_element(k, ".//*[@id=\"tr_{}\"]/td[4]".format(id))
    house.space = get_text_from_element(k, ".//*[@id=\"tr_{}\"]/td[5]".format(id))
    house.floors = get_text_from_element(k, ".//*[@id=\"tr_{}\"]/td[6]".format(id))
    house.rooms = get_text_from_element(k, ".//*[@id=\"tr_{}\"]/td[7]".format(id))
    house.land = get_text_from_element(k, ".//*[@id=\"tr_{}\"]/td[8]".format(id))
    house.price = get_text_from_element(k, ".//*[@id=\"tr_{}\"]/td[9]".format(id))
    return house


def get_ss_data(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"}
    r = requests.get(url, headers=headers)

    tree = html.fromstring(r.content)
    return tree.xpath("//*[@id=\"filter_frm\"]/table[2]")[0]

def get_ad_list(content, ad_type):
    rows = content.body.findall(".//tr")[0]
    ad_rows = rows.findall("..//td[@class='msg2']/div[@class='d1']/a")

    id_list = []

    for row in ad_rows:
        id_list.append(get_id_from_attrib(row.attrib))

    ad_list = []
    for i in id_list:
        if ad_type == "apartment":
            ad_list.append(find_apartment_by_id(content, i))
        elif ad_type == "house":
            ad_list.append(find_house_by_id(content, i))
        else:
            print("Unknown ad type!")
            sys.exit(1)
    return ad_list

def get_ad_hash(ad):
    return hashlib.sha256(str(ad).encode("utf-8")).hexdigest()

class Push:

    def __init__(self, settings):
        self.client = Client(settings["pushover_user_key"], api_token=settings["pushover_api_token"])
        if settings["pushover-enabled"] == True:
            self.enabled = True
        else:
            self.enabled = False

    def send_pushover_message(self, message):
        if self.enabled:
            print("Sending push message")
            self.client.send_message(message, title="New apartment found by sscom-tracker")
        else:
            print("Push messages not enabled!")


def main():
    with open("settings.json","r") as settings_file:
        settings = json.load(settings_file)

    cache = AdCache()
    p = Push(settings)
    tracking_list = settings["tracking_list"]
    for item in tracking_list:
        print("Looking for type: {}".format(item))
        k = get_ss_data(tracking_list[item]["url"])
        ad_list = get_ad_list(k, item)

        for a in ad_list:
            if cache.is_known(a):
                print("OLD: {} [{}]".format(a,get_ad_hash(a)))
            else:
                print("NEW: {} [{}]".format(a,get_ad_hash(a)))
                if item == "apartment":
                    if a.rooms is None:
                        pass
                    if int(a.rooms) >= tracking_list[item]["filter_room_count"]:
                        print("NEW Classified matching filtering criteria found")
                        p.send_pushover_message(a)
                elif item == "house":
                        print("NEW House found")
                        p.send_pushover_message(a)
                else:
                    print("Not enough rooms ({})".format(a.rooms))



if __name__ == "__main__":
    main()



