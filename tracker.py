#!/usr/bin/env python3
#
# SS.COM Tracker
# Monitor classifieds and send push notifications
# when a new classified is found.
#
# kaspars@fx.lv
#
import json
import random
import sys
import time
from os import CLD_CONTINUED

import click
from loguru import logger

import lib.cache
import lib.datastructures
import lib.objectparser
import lib.objectstore
import lib.push
import lib.retriever
import lib.rssstore
import lib.settings
from lib.display import print_results_to_console
from lib.filter import Filter
from lib.log import func_log, normalize, set_up_logging
from lib.push import send_push
from lib.stats import TrackerStatsSql


@click.group()
def cli():
    pass


@func_log
@cli.command()
@click.option("--debug", is_flag=True, default=False, help="Print DEBUG log to screen")
@click.option("--category", default="*", help="Category of classifieds to update")
def update(debug, category):
    """Update data from RSS feeds."""
    if not category in ["house", "car", "dog", "apartment", "land", "*"]:
        click.echo("Unsupported category")
        sys.exit(1)
    settings = lib.settings.Settings()
    set_up_logging(settings, debug)
    with logger.contextualize(task="Update"):
        logger.info(f"Updating '{category}'...")
        rm = lib.retriever.RetrieverManager(settings)
        rm.update_all(category)
        del(rm) # destructors are unreliable
        logger.info("Updating run complete")


@func_log
@cli.command()
@click.option("--debug", is_flag=True, default=False, help="Print DEBUG log to screen")
@click.option("--all", is_flag=True, default=False, help="Load all files, not just latest ones (default)")
def process(debug, all):
    """Parse and store the downloaded RSS data."""
    settings = lib.settings.Settings()
    set_up_logging(settings, debug)
    with logger.contextualize(task="Process"):
        logger.info(f"Starting processing run...{all=}")
        store = lib.rssstore.RSSStore(settings)
        op = lib.objectparser.ObjectParser()
        object_store = lib.objectstore.ObjectStoreSqlite(settings)
        objects_list = store.load(all)
        for rss_object in objects_list:
            parsed_list = op.parse_object(rss_object)
            logger.debug(
                f"[{rss_object.url_hash[:10]}] RSS object parsed, now writing/updating classifieds"
            )
            for classified in parsed_list:
                if object_store.classified_exists(classified):
                    # makes no sense to update it from rss data. if it exists already, let it be
                    # object_store.update_classified(classified)
                    pass
                else:
                    object_store.write_classified(classified)
        logger.info("Processing run complete")
        del object_store  # exlplicitly deleting object calls its destructor and we are making sure to do that while still within the logging context


@func_log
@cli.command()
@click.option("--debug", is_flag=True, default=False, help="Print DEBUG log to screen")
@click.option("--category", default="*", help="Category of classifieds to view")
def view(debug, category):
    if not category in ["house", "car", "dog", "apartment", "*"]:
        click.echo("Unsupported category")
        sys.exit(1)
    settings = lib.settings.Settings()
    set_up_logging(settings, debug)
    with logger.contextualize(task="View"):
        object_store = lib.objectstore.ObjectStoreSqlite(settings)
        print_results_to_console(object_store.get_all_classifieds(category), category)
        del object_store  # exlplicitly deleting object calls its destructor and we are making sure to do that while still within the logging context


def randomsleep():
    sleep_time = random.choice(range(1, 5))
    logger.trace(f"Sleeping for {sleep_time} seconds")
    time.sleep(sleep_time)


@func_log
@cli.command()
@click.option("--debug", is_flag=True, default=False, help="Print DEBUG log to screen")
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Force Enrichment, even if it has already been done before",
)
def enrich(debug, force):
    """Enrich classfieds using data that was retrieved over HTTP."""
    settings = lib.settings.Settings()
    set_up_logging(settings, debug)
    with logger.contextualize(task="Enricher"):
        logger.debug("Running enricher...")
        object_store = lib.objectstore.ObjectStoreSqlite(settings)
        enricher = lib.objectparser.Enricher()
        # iterate over all saved classifieds, depending on classified type, update attributes with
        # data retrieved by the http retriever
        for classified in object_store.get_all_classifieds("*"):
            if hasattr(classified, "enriched"):
                if classified.enriched:
                    if force:
                        logger.debug(
                            f"[{classified.short_hash}] Begin forced enrichment..."
                        )
                        classified = enricher.enrich(classified)
                        object_store.update_classified(classified)
                        logger.debug(
                            f"[{classified.short_hash}] Forced enrichment complete"
                        )
                    else:
                        logger.trace(
                            f"Object {classified.short_hash} has been enriched"
                        )
                else:
                    logger.debug(
                        f"[{classified.short_hash}] Attribute present but is not True"
                    )
                    logger.debug(f"[{classified.short_hash}] Begin enrichment...")
                    classified = enricher.enrich(classified)
                    if classified:
                        logger.debug(f"[{classified.short_hash}] Enrichment complete")
                        object_store.update_classified(classified)
            else:
                if hasattr(classified, "http_response_data"):
                    logger.debug(f"[{classified.short_hash}] Begin enrichment...")
                    classified = enricher.enrich(classified)
                    if classified:
                        logger.debug(f"[{classified.short_hash}] Enrichment complete")
                        object_store.update_classified(classified)
                else:
                    logger.debug(
                        f"[{classified.short_hash}] missing http response data, cannot enrich"
                    )
        del object_store


@func_log
@cli.command()
@click.option("--debug", is_flag=True, default=False, help="Print DEBUG log to screen")
def stats(debug):
    """Update statistics."""
    settings = lib.settings.Settings()
    set_up_logging(settings, debug)
    with logger.contextualize(task="Statistics"):
        logger.debug("Running statistics generator...")
        object_store = lib.objectstore.ObjectStoreSqlite(settings)
        # lets generate some statistics
        count_all = 0
        enriched_count = 0
        count_has_http_response_data = 0
        for classified in object_store.get_all_classifieds("*"):
            count_all += 1
            if hasattr(classified, "http_response_data"):
                if classified.http_response_data != None:
                    count_has_http_response_data += 1
            if hasattr(classified, "enriched"):
                if classified.enriched:
                    enriched_count += 1
        stats = TrackerStatsSql(settings)
        total = 0
        for category in stats.data.categories:
            count = object_store.get_classified_count(category)
            total += count
            stats.set_objects_files_count(category, count)
        stats.set_objects_files_count("total", total)
        stats.set_http_data_stats(count_all, count_has_http_response_data)
        stats.set_enrichment_stats(count_all, enriched_count)
        print(stats.data.enrichment_data)
        logger.debug(
            f"Stats. Classifieds: {count_all} With HTTP data: {count_has_http_response_data} Enriched: {enriched_count}"
        )


@func_log
@cli.command()
@click.option("--debug", is_flag=True, default=False, help="Print DEBUG log to screen")
@click.option("--force", is_flag=True, default=False, help="Force the retrieval, even if it was already previously retrieved")
def retr(debug, force):
    """Retrieve the per-classified details using HTTP."""
    settings = lib.settings.Settings()
    set_up_logging(settings, debug)
    with logger.contextualize(task="URL retriever"):
        logger.debug("Running retriever...")
        object_store = lib.objectstore.ObjectStoreSqlite(settings)
        hr = lib.retriever.HttpRetriever()
        # iterate over all objects
        # for each of them, check if http_response_date is present,
        # if it is not, call HttpRetriever and save the response into the object
        for classified in object_store.get_all_classifieds("*"):
            if hasattr(classified, "http_response_data"):
                logger.trace(
                    f"Object {classified.short_hash} already has http_response_data"
                )
                if classified.http_response_data != None and not force:
                    logger.trace(
                        f"Object {classified.short_hash} already contains http response data, skipping"
                    )
                else:
                    try:
                        http_response = hr.retrieve_ss_data(classified.link)
                    except Exception as e:
                        logger.warning(f"Could not retrieve HTTP data. {e=}")
                        continue
                    classified.http_response_data = http_response.response_content
                    classified.http_response_code = http_response.response_code
                    if classified.category == "apartment":
                        classified.coordinates_string = hr.retrieve_coordinates_from_raw_http_data(http_response.response_raw)
                    object_store.update_classified(classified)
                    # randomsleep()
            else:
                logger.trace(
                    f"Object {classified.short_hash} does not have http_response_data, initiating retrieval"
                )

                http_response = hr.retrieve_ss_data(classified.link)
                classified.http_response_data = http_response.response_content
                classified.http_response_code = http_response.response_code
                object_store.update_classified(classified)
                # randomsleep()
        del object_store


if __name__ == "__main__":
    cli()
