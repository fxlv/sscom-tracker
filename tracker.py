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
import lib.objectparser
import lib.objectstore
import lib.push
import lib.retriever
import lib.rssstore
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
@click.option("--category", default="*", help="Category of classifieds to update")
def update(debug, category):
    if not category in ["house", "car", "dog", "apartment","*"]:
        click.echo("Unsupported category")
        sys.exit(1)
    settings = lib.settings.Settings()
    set_up_logging(settings, debug)
    with logger.contextualize(task="Update"):
        logger.info(f"Updating '{category}'...")
        rm = lib.retriever.RetrieverManager(settings)
        rm.update_all(category)
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
        store = lib.rssstore.RSSStore(settings)
        op = lib.objectparser.ObjectParser()
        object_store = lib.objectstore.ObjectStore(settings)
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
        object_store = lib.objectstore.ObjectStore(settings)
        print_results_to_console(object_store.load_all(category), category)

if __name__ == "__main__":
    cli()
