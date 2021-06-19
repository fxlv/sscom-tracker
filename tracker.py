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

import lib.cache
import lib.datastructures
import lib.push
import lib.retriever
from lib.filter import Filter
from lib.log import func_log, set_up_logging
import click


@func_log
@click.command()
@click.option("--debug", is_flag=True, default=False, help="Print DEBUG log to screen")
def main(debug):

    set_up_logging(debug)
    with open("settings.json") as settings_file:
        settings = json.load(settings_file)

    cache = lib.cache.Cache(settings)
    data_cache = lib.cache.DataCache(settings)

    p = lib.push.Push(settings)
    retriever = lib.retriever.Retriever(settings, data_cache)
    classified_filter = Filter(retriever, cache, settings)
    # get data

    if not data_cache.is_fresh():
        retriever.update_data_cache()
    results = classified_filter.filter_tracking_list()
    # display results and send push notifications
    for classified_type in results:
        print("{} Results for: {} {}".format("=" * 10, classified_type, "=" * 10))
        if len(results[classified_type]["old"]) > 0:
            print("=> OLD/Known classifieds:")
            for r in results[classified_type]["old"]:
                print(r)
            print()
        else:
            print(f"No OLD classifieds for type: {classified_type}")
        if len(results[classified_type]["new"]) > 0:
            print("=> New classifieds:")
            for r in results[classified_type]["new"]:
                print(r)
                push_message = lib.push.PushMessage(r, classified_type)
                p.send_pushover_message(push_message)
            print()
        else:
            print(f"No NEW classifieds for type: {classified_type}")


if __name__ == "__main__":
    main()
