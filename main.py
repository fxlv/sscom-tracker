#!/usr/bin/env python

import requests
from lxml import html
import hashlib
import os
import pickle
from pushover import Client
import json


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




class Ad:

    def __init__(self):
        self.title = None
        self.id = None
        self.street = None

    def __str__(self):
        return "Ad: {} / Str: {} / rooms: {} / floor: {}".format(self.title, self.street, self.rooms, self.floor)

    def __repr__(self):
        return self.__str__()

def get_id_from_attrib(attrib):
    return attrib["id"].split("_")[1]

def get_text_from_element(k, xpath):
    return k.body.findall(xpath)[0].text

def find_by_id(k,id):
    ad = Ad()
    ad.title = get_text_from_element(k, ".//a[@id=\"dm_{}\"]".format(id))
    ad.street = get_text_from_element(k, ".//*[@id=\"tr_{}\"]/td[4]".format(id))
    ad.rooms = get_text_from_element(k, ".//*[@id=\"tr_{}\"]/td[5]".format(id))
    ad.space = get_text_from_element(k, ".//*[@id=\"tr_{}\"]/td[6]".format(id))
    ad.floor = get_text_from_element(k, ".//*[@id=\"tr_{}\"]/td[7]".format(id))
    ad.series = get_text_from_element(k, ".//*[@id=\"tr_{}\"]/td[8]".format(id))
    ad.price_per_m = get_text_from_element(k, ".//*[@id=\"tr_{}\"]/td[9]".format(id))
    ad.price = get_text_from_element(k, ".//*[@id=\"tr_{}\"]/td[10]".format(id))
    return ad

def get_ss_data():
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"}
    r = requests.get("https://www.ss.com/lv/real-estate/flats/riga/teika/today-2/sell/", headers=headers)
    tree = html.fromstring(r.content)
    return tree.xpath("//*[@id=\"filter_frm\"]/table[2]")[0]

def get_ad_list(content):
    rows = content.body.findall(".//tr")[0]
    ad_rows = rows.findall("..//td[@class='msg2']/div[@class='d1']/a")

    id_list = []

    for row in ad_rows:
        id_list.append(get_id_from_attrib(row.attrib))

    ad_list = []
    for i in id_list:
        ad_list.append(find_by_id(content, i))
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
    k = get_ss_data()
    ad_list = get_ad_list(k)

    for a in ad_list:
        if cache.is_known(a):
            print("OLD: {} [{}]".format(a,get_ad_hash(a)))
        else:
            print("NEW: {} [{}]".format(a,get_ad_hash(a)))
            if int(a.rooms) >= settings["filter_room_count"]:
                print("NEW Ad matchinng filtering criteria found")
                p.send_pushover_message(a)
            else:
                print("Not enough rooms ({})".format(a.rooms))



if __name__ == "__main__":
    main()



