#!/usr/bin/env python3
#
# SS.COM Tracker
# Monitor classifieds and send push notifications
# when a new classified is found.
#
# kaspars@fx.lv
#
import argparse
import json
import logging
import sys
import time

import requests
from lxml import html

import lib.cache
import lib.push
from lib.datastructures import Apartment, House


def func_log(function_name):
    """Decorator for logging and timing function execution."""

    def log_it(*args, **kwargs):
        """Log function and its args, execute the function and return the result."""
        t_start = time.time()
        result = function_name(*args, **kwargs)
        t_end = time.time() - t_start
        msg = "Function call: {}".format(function_name.__name__)
        if args:
            msg += " with args: {}".format(args)
        if kwargs:
            msg += " with kwargs {}".format(args, kwargs)
        msg += " executed in: {:5.5f} sec".format(t_end)
        logging.debug(msg)
        return result

    return log_it


def get_id_from_attrib(attrib):
    return attrib["id"].split("_")[1]


def get_text_from_element(k, xpath):
    return k.body.findall(xpath)[0].text


def find_apartment_by_id(k, id):

    title = get_text_from_element(k,
                                            ".//a[@id=\"dm_{}\"]".format(id))
    street = get_text_from_element(
        k, ".//*[@id=\"tr_{}\"]/td[4]".format(id))

    if title is None or street is None:
        logging.warning("Invalid data for classified with ID: {}".format(id))
        return False
    apartment = Apartment(title, street)

    apartment.rooms = get_text_from_element(
        k, ".//*[@id=\"tr_{}\"]/td[5]".format(id))
    apartment.space = get_text_from_element(
        k, ".//*[@id=\"tr_{}\"]/td[6]".format(id))
    apartment.floor = get_text_from_element(
        k, ".//*[@id=\"tr_{}\"]/td[7]".format(id))
    apartment.series = get_text_from_element(
        k, ".//*[@id=\"tr_{}\"]/td[8]".format(id))
    apartment.price_per_m = get_text_from_element(
        k, ".//*[@id=\"tr_{}\"]/td[9]".format(id))
    apartment.price = get_text_from_element(
        k, ".//*[@id=\"tr_{}\"]/td[10]".format(id))
    return apartment


def find_house_by_id(k, id):
    house = House()
    house.title = get_text_from_element(k, ".//a[@id=\"dm_{}\"]".format(id))
    house.street = get_text_from_element(
        k, ".//*[@id=\"tr_{}\"]/td[4]".format(id))
    house.space = get_text_from_element(k,
                                        ".//*[@id=\"tr_{}\"]/td[5]".format(id))
    house.floors = get_text_from_element(
        k, ".//*[@id=\"tr_{}\"]/td[6]".format(id))
    house.rooms = get_text_from_element(k,
                                        ".//*[@id=\"tr_{}\"]/td[7]".format(id))
    house.land = get_text_from_element(k,
                                       ".//*[@id=\"tr_{}\"]/td[8]".format(id))
    house.price = get_text_from_element(k,
                                        ".//*[@id=\"tr_{}\"]/td[9]".format(id))
    return house

@func_log
def retrieve_ss_data(url: str) -> bool:
    """Retrieve SS.COM data and store in cache.

    Retrieve the data using the URL provided.
    Store it in the cache and return bool result.
    """

    #hashlib.sha256(str(self).encode("utf-8")).hexdigest()
    headers = {
        "User-Agent":
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"
    }
    r = requests.get(url, headers=headers)

    tree = html.fromstring(r.content)
    return tree.xpath("//*[@id=\"filter_frm\"]/table[2]")[0]


@func_log
def get_ss_data(url: str) -> object:
    headers = {
        "User-Agent":
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"
    }
    r = requests.get(url, headers=headers)

    tree = html.fromstring(r.content)
    return tree.xpath("//*[@id=\"filter_frm\"]/table[2]")[0]


@func_log
def get_ad_list(content, ad_type):
    rows = content.body.findall(".//tr")[0]
    ad_rows = rows.findall("..//td[@class='msg2']/div[@class='d1']/a")

    id_list = []

    for row in ad_rows:
        id_list.append(get_id_from_attrib(row.attrib))

    ad_list = []
    for i in id_list:
        if ad_type == "apartment":
            apartment = find_apartment_by_id(content, i)
            if not apartment:
                continue # skip items that are False (could happen with malformed input)
            if apartment.rooms and apartment.floor:
                ad_list.append(apartment)
            else:
                logging.debug("Skipping invalid apartment: {}".format(apartment))
        elif ad_type == "house":
            house = find_apartment_by_id(content, i)
            if house.title:
                ad_list.append(house)
            else:
                logging.debug("Skipping invalid house: {}".format(apartment))
        else:
            logging.warning("Unknown classified type!")
            sys.exit(1)
    return ad_list


def parse_user_args():
    """Parse commandline arguments."""
    parser = argparse.ArgumentParser()
    parser.description = "SS.COM Tracker"
    parser.add_argument("--debug",
                        action="store_true",
                        help="Enable DEBUG logging")
    args = parser.parse_args()
    return args


def set_up_logging(debug=False):
    log_format = "%(asctime)s %(levelname)s %(name)s " \
                 "%(filename)s %(lineno)d >> %(message)s"
    if debug:
        logging.basicConfig(level=logging.DEBUG,
                            filename="debug.log",
                            format=log_format)
        logging.debug('Logging started.')
    else:
        logging.basicConfig(level=logging.INFO)

@func_log
def main():
    args = parse_user_args()

    set_up_logging(args.debug)
    with open("settings.json", "r") as settings_file:
        settings = json.load(settings_file)

    cache = lib.cache.Cache(settings)
    p = lib.push.Push(settings)
    tracking_list = settings["tracking_list"]
    for item in tracking_list:
        logging.info("Looking for type: {}".format(item))
        k = get_ss_data(tracking_list[item]["url"])
        ad_list = get_ad_list(k, item)

        for a in ad_list:

            if cache.is_known(a):
                print("OLD: {} [{}]".format(a, a.get_hash()))
            else:
                cache.add(a)
                print("NEW: {} [{}]".format(a, a.get_hash()))
                if item == "apartment":
                    if int(a.rooms
                           ) >= tracking_list[item]["filter_room_count"]:
                        logging.debug(
                            "NEW Classified matching filtering criteria found")
                        p.send_pushover_message(a)
                elif item == "house":
                    logging.info("NEW House found")
                    p.send_pushover_message(a)
                else:
                    logging.info("Not enough rooms ({})".format(a.rooms))


if __name__ == "__main__":
    main()
