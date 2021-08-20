#!/usr/bin/env python3
#
# SS.COM Tracker
# Monitor classifieds and send push notifications
# when a new classified is found.
#
# kaspars@fx.lv
#
import json
import sys

import click
import lib.cache
import lib.datastructures
import lib.push
import lib.retriever
from lib.display import print_results_to_console
from lib.filter import Filter
import lib.settings
from lib.log import func_log, set_up_logging
from lib.push import send_push
from loguru import logger


@click.group()
def cli():
    pass


@func_log
@cli.command()
@click.option("--debug", is_flag=True, default=False, help="Print DEBUG log to screen")
def update(debug):
    settings = lib.settings.Settings()
    set_up_logging(settings, debug)
    with logger.contextualize(task="Update"):
        logger.info("Updating...")
        rm = lib.retriever.RetrieverManager(settings)
        rm.update_all()
        logger.info("Updating run complete")


@func_log
@cli.command()
@click.option("--debug", is_flag=True, default=False, help="Print DEBUG log to screen")
def process(debug):
    """Parse and store the downloaded RSS data."""
    settings = lib.settings.Settings()
    set_up_logging(settings, debug)
    with logger.contextualize(task="Process"):
        logger.info("Starting processing run...")
        store = lib.retriever.RSSStore(settings)
        op = lib.retriever.ObjectParser()
        object_store = lib.retriever.ObjectStore(settings)
        objects_list = store.load_all()
        for rss_object in objects_list:
            parsed_list = op.parse_object(rss_object)
            logger.debug(f"[{rss_object.url_hash[:10]}] RSS object parsed, now writing/updating classifieds")
            for classified in parsed_list:
                object_store.write(classified)
        logger.info("Processing run complete")


@func_log
@cli.command()
@click.option("--debug", is_flag=True, default=False, help="Print DEBUG log to screen")
@click.option("--category", default="*", help="Category of classifieds to view")
def view(debug, category):
    if not category in ["house", "car", "dog", "apartment","*"]:
        click.echo("Unsupported category")
        sys.exit(1)
    settings = lib.settings.Settings()
    set_up_logging(settings, debug)
    with logger.contextualize(task="View"):
        object_store = lib.retriever.ObjectStore(settings)
        print_results_to_console(object_store.load_all(category), category)



@func_log
@cli.command()
@click.option("--debug", is_flag=True, default=False, help="Print DEBUG log to screen")
@click.option("--print/--no-print", default=True, help="Print results to console")
@click.option("--push/--no-push", default=False, help="Send push notifications")
def print(debug, print, push):

    settings = lib.settings.Settings()
    set_up_logging(settings, debug)

    cache = lib.cache.Cache(settings)
    data_cache = lib.cache.DataCache(settings)

    retriever = lib.retriever.Retriever(settings, data_cache)
    classified_filter = Filter(retriever, cache, settings)

    if not data_cache.is_fresh():
        retriever.update_data_cache()
    results = classified_filter.filter_tracking_list()

    if print:
        print_results_to_console(results)

    if push:
        send_push(settings, results)


if __name__ == "__main__":
    cli()
