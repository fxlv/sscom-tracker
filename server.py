import json
import sys

import click
import lib.cache
import lib.datastructures
import lib.objectstore
import lib.push
import lib.retriever
import lib.rssstore
import lib.stats
from lib.display import print_results_to_console
from lib.filter import Filter
import lib.settings
from lib.log import func_log, set_up_logging
from lib.push import send_push
from loguru import logger
from flask import render_template
from flask import Flask, request



app = Flask(__name__)

@app.route("/")
def index():
    with logger.contextualize(task="Web->Index"):
        logger.debug("Returning index")
        settings = lib.settings.Settings()
        set_up_logging(settings)
        object_store = lib.objectstore.ObjectStore(settings)
        rss_store = lib.rssstore.RSSStore(settings)
        stats = lib.stats.TrackerStats(settings)
        print(stats.data.enrichment_data)
        return render_template("index.html", stats=stats, classifieds = object_store.load_all())

@app.route("/category/<category>")
def category(category=None):
    with logger.contextualize(task="Web->Category"):
        logger.debug(f"Returning category view for {category}")
        settings = lib.settings.Settings()
        set_up_logging(settings)
        object_store = lib.objectstore.ObjectStore(settings)
        stats = lib.stats.TrackerStats(settings)
        return render_template("category.html", stats=stats, category=category, classifieds = object_store.load_all(category))

@app.route("/category/<category>/<hash>")
def classified(category=None, hash=None):
    with logger.contextualize(task="Web->Classified"):
        logger.debug(f"Viewing classified {hash} from category {category}")
        settings = lib.settings.Settings()
        set_up_logging(settings)
        object_store = lib.objectstore.ObjectStore(settings)
        return render_template("classified.html", category=category, classified = object_store.get_object_by_hash(category, hash))

@app.route('/retrieve', methods=['POST'])
def retrieve():
    data = request.form
    print(data["url"])
    settings = lib.settings.Settings()
    return "Your request has been added to the queue."

