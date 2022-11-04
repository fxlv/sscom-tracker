from flask import Flask, request, jsonify
from flask import render_template
from loguru import logger

import re
import lib.cache
import lib.datastructures
import lib.objectstore
import lib.push
import lib.retriever
import lib.rssstore
import lib.settings
import lib.stats
from lib.log import set_up_logging
from lib.zabbix import Zabbix, ZabbixStopwatch
from devtools import debug

app = Flask(__name__, static_url_path='/static')


@app.route("/")
def index():
    with logger.contextualize(task="Web->Index"):
        logger.debug("Returning index")
        settings = lib.settings.Settings()
        z = Zabbix(settings)
        render_stopwatch = ZabbixStopwatch(z, "index_page_rendering_time_seconds")
        set_up_logging(settings)
        object_store = lib.objectstore.ObjectStoreSqlite(settings)
        rss_store = lib.rssstore.RSSStore(settings)
        stats = lib.stats.TrackerStatsSql(settings)
        result = render_template("index.html", stats=stats)
        render_stopwatch.done()
        return result


@app.route("/apartments")
@app.route("/apartments/by-city/<city>")
def apartments(city=None):
    with logger.contextualize(task="Web->Apartments"):
        logger.debug("Returning apartments view")
        settings = lib.settings.Settings()
        z = Zabbix(settings)
        render_stopwatch = ZabbixStopwatch(z, "apartments_page_rendering_time_seconds")
        set_up_logging(settings)
        object_store = lib.objectstore.ObjectStoreSqlite(settings)
        stats = lib.stats.TrackerStatsSql(settings)
        result = render_template("apartments.html",
                                 stats=stats,
                                 category="apartment",
                                 city=city,
                                 city_coordinates=object_store.get_city_coordinates(city),
                                 classifieds=object_store._get_latest_apartments(city=city))
        render_stopwatch.done()
        return result

@app.route("/cars")
@app.route("/cars/model/<model>")
def cars(model=None):
    with logger.contextualize(task="Web->Cars"):
        logger.debug("Returning cars view")
        settings = lib.settings.Settings()
        z = Zabbix(settings)
        set_up_logging(settings)
        result = render_template("car.html",
                                 model=model)
        return result

@app.route("/category/<category>")
@app.route("/category/ordered/<category>/<order_by>")
def category(category=None, order_by=None):
    if order_by:
        if order_by not in ["mileage", "price"]:
            order_by = None  # basic attempt at filtering
    with logger.contextualize(task="Web->Category"):
        logger.debug(f"Returning category view for {category}")
        settings = lib.settings.Settings()
        z = Zabbix(settings)
        render_stopwatch = ZabbixStopwatch(z, "category_page_rendering_time_seconds")
        set_up_logging(settings)
        object_store = lib.objectstore.ObjectStoreSqlite(settings)
        stats = lib.stats.TrackerStatsSql(settings)
        result = render_template(
            "category.html",
            stats=stats,
            category=category,
            classifieds=object_store.get_latest_classifieds(category, order_by),
        )
        render_stopwatch.done()
        return result
@app.route("/json/category/<category>")
@app.route("/json/category/<category>/model/<model>")
@app.route("/json/category/ordered/<category>/<order_by>")
def json_category(category=None, order_by=None, model=None):
    if order_by:
        if order_by not in ["mileage", "price"]:
            order_by = None  # basic attempt at filtering
    with logger.contextualize(task="Web->Category"):
        logger.debug(f"Returning JSON data for {category}")
        settings = lib.settings.Settings()
        z = Zabbix(settings)
        debug(request.args)
        render_stopwatch = ZabbixStopwatch(z, "category_page_rendering_time_seconds")
        set_up_logging(settings)
        object_store = lib.objectstore.ObjectStoreSqlite(settings)
        stats = lib.stats.TrackerStatsSql(settings)
        if category == "car":
            debug(model)
            classifieds = object_store._get_cars(model_filter=model)
        else:
            classifieds=object_store.get_latest_classifieds(category, order_by),
        if category == "land":
            classifieds = [ {"title":c.title,"price":c.price,"link": c.link,"street":c.street, "published": c.published.humanize()} for c in classifieds[0]]
        if category == "car":
            classifieds = [ {"title":c.title,"price":c.price,"link": c.link,"model":c.model, "mileage": c.mileage, "engine":c.engine, "published": c.published.humanize()} for c in classifieds]
        render_stopwatch.done()
        return jsonify(classifieds)


@app.route("/category/<category>/all")
def category_all(category=None):
    with logger.contextualize(task="Web->Category"):
        logger.debug(f"Returning category view for {category}")
        settings = lib.settings.Settings()
        z = Zabbix(settings)
        render_stopwatch = ZabbixStopwatch(z, "category_all_page_rendering_time_seconds")
        set_up_logging(settings)
        object_store = lib.objectstore.ObjectStoreSqlite(settings)
        stats = lib.stats.TrackerStatsSql(settings)
        result = render_template(
            "category.html",
            stats=stats,
            category=category,
            classifieds=object_store.get_all_classifieds(category),
        )
        render_stopwatch.done()
        return result


@app.route("/category/<category>/<hash>")
def classified(category=None, hash=None):
    with logger.contextualize(task="Web->Classified"):
        logger.debug(f"Viewing classified {hash} from category {category}")
        settings = lib.settings.Settings()
        z = Zabbix(settings)
        render_stopwatch = ZabbixStopwatch(z, "classified_page_rendering_time_seconds")
        set_up_logging(settings)
        object_store = lib.objectstore.ObjectStoreSqlite(settings)
        result = render_template(
            "classified.html",
            category=category,
            classified=object_store.get_classified_by_category_hash(category, hash),
        )
        render_stopwatch.done()
        return result


@app.route("/retrieve", methods=["POST"])
def retrieve():
    data = request.form
    print(data["url"])
    settings = lib.settings.Settings()
    return "Your request has been added to the queue."
