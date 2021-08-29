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
import time
import click
import lib.cache
import lib.datastructures
import lib.objectparser
import lib.objectstore
import lib.push
import lib.retriever
import lib.rssstore
from lib.stats import TrackerStats
from lib.display import print_results_to_console
from lib.filter import Filter
import lib.settings
from lib.log import func_log, set_up_logging, normalize
from lib.push import send_push
from loguru import logger
import random

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


def randomsleep():
    sleep_time = random.choice(range(1, 5))
    logger.trace(f"Sleeping for {sleep_time} seconds")
    time.sleep(sleep_time)


@func_log
@cli.command()
@click.option("--debug", is_flag=True, default=False, help="Print DEBUG log to screen")
@click.option("--force", is_flag=True, default=False, help="Force Enrichment, even if it has already been done before")
def enrich(debug, force):
    settings = lib.settings.Settings()
    set_up_logging(settings, debug)
    with logger.contextualize(task="Enricher"):
        logger.debug("Running enricher...")
        object_store = lib.objectstore.ObjectStore(settings)
        enricher = lib.objectparser.Enricher()
        # iterate over all saved classifieds, depending on classified type, update attributes with
        # data retrieved by the http retriever
        for classified in object_store.load_all("*"):
            if hasattr(classified, "enriched"):
                if classified.enriched:
                    if force:
                        logger.debug(f"[{classified.short_hash}] Begin forced enrichment...")
                        classified = enricher.enrich(classified)
                        object_store.update(classified)
                        logger.debug(f"[{classified.short_hash}] Forced enrichment complete")
                    else:
                        logger.trace(f"Object {classified.short_hash} has been enriched")
                else:
                    logger.debug(f"[{classified.short_hash}] Attribute present but is not True")
            else:
                if hasattr(classified, "http_response_data"):
                    logger.debug(f"[{classified.short_hash}] Begin enrichment...")
                    classified = enricher.enrich(classified)
                    logger.debug(f"[{classified.short_hash}] Enrichment complete")
                    object_store.update(classified)
                else:
                    logger.debug(f"[{classified.short_hash}] missing http response data, cannot enrich")

@func_log
@cli.command()
@click.option("--debug", is_flag=True, default=False, help="Print DEBUG log to screen")
def stats(debug):
    settings = lib.settings.Settings()
    set_up_logging(settings, debug)
    with logger.contextualize(task="Statistics"):
        logger.debug("Running statistics generator...")
        object_store = lib.objectstore.ObjectStore(settings)
        # lets generate some statistics
        count_all = 0
        enriched_count = 0
        count_has_http_response_data = 0
        for classified in object_store.load_all("*"):
            count_all+=1
            if hasattr(classified, "http_response_data"):
                count_has_http_response_data+=1
            if hasattr(classified, "enriched"):
                if classified.enriched:
                    enriched_count+=1
        # now we need to force the closing of the object store, as it also manipulates stats
        del(object_store)
        stats = TrackerStats(settings)
        stats.set_http_data_stats(count_all, count_has_http_response_data)
        stats.set_enrichment_stats(count_all, enriched_count)
        print(stats.data.enrichment_data)
        logger.debug(f"Stats. Classifieds: {count_all} With HTTP data: {count_has_http_response_data} Enriched: {enriched_count}")


@func_log
@cli.command()
@click.option("--debug", is_flag=True, default=False, help="Print DEBUG log to screen")
def retr(debug):
    settings = lib.settings.Settings()
    set_up_logging(settings, debug)
    with logger.contextualize(task="URL retriever"):
        logger.debug("Running retriever...")
        object_store = lib.objectstore.ObjectStore(settings)
        hr = lib.retriever.HttpRetriever()
        # iterate over all objects
        # for each of them, check if http_response_date is present,
        # if it is not, call HttpRetriever and save the response into the object
        for classified in object_store.load_all("*"):
            if hasattr(classified, "http_response_data"):
                logger.trace(f"Object {classified.short_hash} already has http_response_data")
                if classified.http_response_data != None:
                    logger.trace(f"Object {classified.short_hash} already has http response data, skipping")
                else:
                    classified.http_response_data = hr.retrieve_ss_data(classified.link)
                    object_store.update(classified)
                    randomsleep()
            else:
                logger.trace(f"Object {classified.short_hash} does not have http_response_data, initiating retrieval")
                classified.http_response_data = hr.retrieve_ss_data(classified.link)
                object_store.update(classified)
                randomsleep()

if __name__ == "__main__":
    cli()
