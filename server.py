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
from flask import render_template
from flask import Flask



app = Flask(__name__)

@app.route("/")
def index():
    with logger.contextualize(task="Web"):
        logger.debug("Returning index")
        settings = lib.settings.Settings()
        set_up_logging(settings)
        object_store = lib.retriever.ObjectStore(settings)
        rss_store = lib.retriever.RSSStore(settings)
        stats = lib.retriever.TrackerStats(settings, object_store, rss_store)
        return render_template("index.html", stats=stats, classifieds = object_store.load_all())

@app.route("/category/<category>")
def category(category=None):
    with logger.contextualize(task="Web"):
        logger.debug(f"Returning category view for {category}")
        settings = lib.settings.Settings()
        set_up_logging(settings)
        object_store = lib.retriever.ObjectStore(settings)
        rss_store = lib.retriever.RSSStore(settings)
        stats = lib.retriever.TrackerStats(settings, object_store, rss_store)
        return render_template("category.html", stats=stats, category=category, classifieds = object_store.load_all(category))

