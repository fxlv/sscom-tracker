#!/usr/bin/env python3
#
# SS.COM Tracker
# Monitor classifieds and send push notifications
# when a new classified is found.
#
# kaspars@fx.lv
#
import json
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


@click.group()
def cli():
    pass


@func_log
@cli.command()
@click.option("--debug", is_flag=True, default=False, help="Print DEBUG log to screen")
def update(debug):
    settings = lib.settings.Settings()
    set_up_logging(settings, debug)
    click.echo("Updating...")
    rm = lib.retriever.RetrieverManager(settings)
    rm.update_all()

@func_log
@cli.command()
@click.option("--debug", is_flag=True, default=False, help="Print DEBUG log to screen")
def view(debug):
    settings = lib.settings.Settings()
    set_up_logging(settings, debug)
    pass # to be implemented

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
