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

import lib.cache
import lib.push

import lib.retriever
from lib.log import func_log, set_up_logging


def parse_user_args():
    """Parse commandline arguments."""
    parser = argparse.ArgumentParser()
    parser.description = "SS.COM Tracker"
    parser.add_argument("--debug",
                        action="store_true",
                        help="Enable DEBUG logging")
    args = parser.parse_args()
    return args


@func_log
def main():
    args = parse_user_args()

    set_up_logging(args.debug)
    with open("settings.json", "r") as settings_file:
        settings = json.load(settings_file)

    cache = lib.cache.Cache(settings)
    p = lib.push.Push(settings)
    tracking_list = settings["tracking_list"]
    retriever = lib.retriever.Retriever(settings)
    if not retriever.is_cache_fresh():
        retriever.update_data_cache()

    for item in tracking_list:
        logging.info("Looking for type: {}".format(item))
        k = retriever.get_ss_data_from_cache (tracking_list[item]["url"])
        ad_list = retriever.get_ad_list(k, item)

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
