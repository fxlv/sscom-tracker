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


app = Flask(__name__, static_url_path='/static')


@app.route("/")
def index():
    with logger.contextualize(task="Web->Index"):
        logger.debug("Returning index")
        settings = lib.settings.Settings()
        set_up_logging(settings)
        object_store = lib.objectstore.ObjectStoreSqlite(settings)
        rss_store = lib.rssstore.RSSStore(settings)
        stats = lib.stats.TrackerStatsSql(settings)
        return render_template("index.html", stats=stats)

@app.route("/apartments")
@app.route("/apartments/by-city/<city>")
def apartments(city=None):
    with logger.contextualize(task="Web->Apartments"):
        logger.debug("Returning apartments view")
        settings = lib.settings.Settings()
        set_up_logging(settings)
        object_store = lib.objectstore.ObjectStoreSqlite(settings)
        stats = lib.stats.TrackerStatsSql(settings)
        return render_template("apartments.html",
            stats=stats,
            category="apartment",
            city = city,
            city_coordinates = object_store.get_city_coordinates(city),
            classifieds=object_store._get_latest_apartments(city=city))

@app.route("/category/<category>")
@app.route("/category/ordered/<category>/<order_by>")
def category(category=None, order_by=None):
    if order_by:
        if order_by not in [ "mileage", "price"]:
            order_by = None # basic attempt at filtering
    with logger.contextualize(task="Web->Category"):
        logger.debug(f"Returning category view for {category}")
        settings = lib.settings.Settings()
        set_up_logging(settings)
        object_store = lib.objectstore.ObjectStoreSqlite(settings)
        stats = lib.stats.TrackerStatsSql(settings)
        return render_template(
            "category.html",
            stats=stats,
            category=category,
            classifieds=object_store.get_latest_classifieds(category, order_by),
        )

@app.route("/category/<category>/all")
def category_all(category=None):
    with logger.contextualize(task="Web->Category"):
        logger.debug(f"Returning category view for {category}")
        settings = lib.settings.Settings()
        set_up_logging(settings)
        object_store = lib.objectstore.ObjectStoreSqlite(settings)
        stats = lib.stats.TrackerStatsSql(settings)
        return render_template(
            "category.html",
            stats=stats,
            category=category,
            classifieds=object_store.get_all_classifieds(category),
        )

@app.route("/category/car/filter/<model>")
@app.route("/category/car/filter/<model>/<order_by>")
@app.route("/category/car/filter/<model>/<order_by>/<debug>")
def categoryfilter(model=None, order_by=None, debug=False):
    with logger.contextualize(task="Web->Category"):
        if debug == "debug":
            debug = True
        else:
            debug = False
        category = "car"
        model = model.replace("_", " ")
        logger.debug(
            f"Returning filtered category view for {category} and model {model}"
        )
        settings = lib.settings.Settings()
        set_up_logging(settings)
        object_store = lib.objectstore.ObjectStoreSqlite(settings)
        stats = lib.stats.TrackerStatsSql(settings)
        classifieds = object_store.get_all_classifieds(category, order_by=order_by)
        # check that model is not None and then filter by comparing lowercase
        # versions of models in storage and the model user supplied
        classifieds = [
            c
            for c in classifieds
            if c.model is not None and c.model.lower() == model.lower()
        ]
        return render_template(
            "category.html",
            stats=stats,
            category=category,
            model=model,
            classifieds=classifieds,
            debug=debug,
        )


@app.route("/category/<category>/<hash>")
def classified(category=None, hash=None):
    with logger.contextualize(task="Web->Classified"):
        logger.debug(f"Viewing classified {hash} from category {category}")
        settings = lib.settings.Settings()
        set_up_logging(settings)
        object_store = lib.objectstore.ObjectStoreSqlite(settings)
        return render_template(
            "classified.html",
            category=category,
            classified=object_store.get_classified_by_category_hash(category, hash),
        )


@app.route("/retrieve", methods=["POST"])
def retrieve():
    data = request.form
    print(data["url"])
    settings = lib.settings.Settings()
    return "Your request has been added to the queue."
