import datetime
import pickle
from re import L
import sqlite3
from pathlib import Path
from sys import breakpointhook
from unittest import result
from xml.dom.expatbuilder import ParseEscape
import lib.objectstore
import arrow
import portalocker
from loguru import logger

import lib.settings
import lib.zabbix




def generate_stats(settings: lib.settings.Settings):
    logger.debug("Running statistics generator...")
    object_store = lib.objectstore.ObjectStoreSqlite(settings)
    # lets generate some statistics
    count_all = 0
    enriched_count = 0
    count_has_http_response_data = 0
    stats = TrackerStatsSql(settings)
    total = 0
    for category in stats.data.categories:
        count = object_store.get_classified_count(category)
        total += count
        stats.set_objects_files_count(category, count)
    count_has_http_response_data = 0
    count_has_http_response_data+= object_store._get_count_http_data_land_classifieds()
    count_has_http_response_data+= object_store._get_count_http_data_cars_classifieds()
    count_has_http_response_data+= object_store._get_count_http_data_houses_classifieds()
    count_has_http_response_data+= object_store._get_count_http_data_apartments_classifieds()
    enriched_count+= object_store._get_count_enriched_land_classifieds()
    enriched_count+= object_store._get_count_enriched_cars_classifieds()
    enriched_count+= object_store._get_count_enriched_houses_classifieds()
    enriched_count+= object_store._get_count_enriched_apartments_classifieds()

    stats.set_objects_files_count("total", total)
    stats.set_http_data_stats(count_all, count_has_http_response_data)
    stats.set_enrichment_stats(count_all, enriched_count)
    print(stats.data.enrichment_data)
    logger.debug(
        f"Stats. Classifieds: {count_all} With HTTP data: {count_has_http_response_data} Enriched: {enriched_count}"
    )
class StatsData:
    def __init__(self):
        self.objects_files_count = {}
        self.rss_files_count = None
        self.last_rss_update = None
        self.last_objects_update = None
        self.last_retrieval_run = None
        self.last_enricher_run = None
        self.categories = []
        self.http_data = None, None
        self.enrichment_data = None, None


class TrackerStatsSql:
    def __init__(self, settings: lib.settings.Settings):
        self.settings = settings
        self.con = sqlite3.connect(self.settings.sqlite_db)
        self.cur = self.con.cursor()
        self.last_objects_update_timestamp = None # used for throttling
        self.data = StatsData()
        self.data.categories = list(self.settings.tracking_list.keys())
        self.zabbix = lib.zabbix.Zabbix(settings)
        logger.trace("TrackerStatsSql: __init__")

    def _enforce_timestamp(self, timestamp_value):
        "Throw exception in case 'timestamp_value' is not a valid timestamp"
        if isinstance(timestamp_value, datetime.datetime):
            return True
        if isinstance(timestamp_value, arrow.Arrow):
            return True
        raise ValueError("Timestamp value is not a valid timestamp")

    def set_last_rss_update(self, timestamp: datetime.datetime):
        logger.trace("TrackerStatsSql: set_last_rss_update")
        self._enforce_timestamp(timestamp)
        sql = "insert into stats_last_rss_update (last_update_timestamp) values (?)"
        sql_data = (timestamp,)
        self.cur.execute(sql, sql_data)
        self.con.commit()

    def get_last_rss_update(self) -> datetime.datetime:
        logger.trace("TrackerStatsSql: get_last_rss_update")
        sql = "select last_update_timestamp from stats_last_rss_update order by id desc limit 1"
        self.cur.execute(sql)
        last_rss_update = self.cur.fetchone()[0]
        return arrow.get(last_rss_update)

    def set_rss_files_count(self, count: int):
        logger.trace("TrackerStatsSql: set_rss_files_count")
        self._enforce_acceptable_count_values(count)
        sql = "insert into stats_rss_files_count (rss_files_count, last_update_timestamp) values (?,?)"
        timestamp = arrow.now().datetime
        sql_data = (count, timestamp,)
        self.cur.execute(sql, sql_data)
        self.con.commit()
        self.zabbix.send_int_to_zabbix("rss_files_count", count)
    
    def mark_rss_file_as_parsed(self, rss_file_name: str):
        logger.trace("TrackerStatsSql: mark_rss_file_as_parsed")
        sql = "insert into stats_processed_rss_objects_list (processed_time, rss_file_name ) values (?,?)"
        timestamp = arrow.now().datetime
        sql_data = (timestamp,rss_file_name,)
        self.cur.execute(sql, sql_data)
        self.con.commit()


    def check_if_rss_file_was_parsed(self, rss_file_name: str) -> bool:
        logger.trace("TrackerStatsSql: check_if_rss_file_was_parsed")
        sql = "select count(*) from stats_processed_rss_objects_list where rss_file_name = '%s'"
        self.cur.execute(sql % rss_file_name)
        objects_files_count = self.cur.fetchone()[0]
        if objects_files_count == 1:
            return True
        return False

    def get_rss_files_count(self) -> int:
        logger.trace("TrackerStatsSql: get_rss_files_count")
        sql = "select rss_files_count from stats_rss_files_count order by id desc limit 1"
        self.cur.execute(sql)
        rss_files_count = self.cur.fetchone()[0]
        return rss_files_count

    def _enforce_acceptable_count_values(self, integer: int):
        """
        Enforcing min/max acceptable values for integers.

        Max acceptable value is 9223372036854775807 (due to sqlite3) and minimal is 0
        as there is no case I can imagine why would we ever set count to negative value.
        """
        if integer > 9223372036854775807:
            raise ValueError("Files count integer is too big for sqlite3")
        if integer < 0:
            raise ValueError("Count cannot be negative.")

    def set_objects_files_count(self, category: str, count: int):
        logger.trace("TrackerStatsSql: set_objects_files_count")
        self._enforce_acceptable_count_values(count)
        sql = "insert into stats_objects_files_count (objects_files_count,category,last_update_timestamp) values (?,?,?)"
        timestamp = arrow.now().datetime
        sql_data = (count, category, timestamp,)
        self.cur.execute(sql, sql_data)
        self.con.commit()
        self.zabbix.send_int_to_zabbix(f"objects_{category}_count", count)

    def get_objects_files_count(self, category: str) -> int:
        logger.trace("TrackerStatsSql: get_objects_files_count")
        sql = "select objects_files_count from stats_objects_files_count where category = '%s' order by id desc limit 1"
        self.cur.execute(sql % category)
        objects_files_count = self.cur.fetchone()[0]
        return objects_files_count

    def get_all_objects_files_count(self) -> int:
        logger.trace("TrackerStatsSql: get_objects_files_count")
        total = 0
        for category in self.data.categories:
            sql = "select objects_files_count from stats_objects_files_count where category = '%s' order by id desc limit 1"
            self.cur.execute(sql % category)
            objects_files_count = self.cur.fetchone()[0]
            total += objects_files_count
        return total

    def set_http_data_stats(self, total_files_count: int, files_with_http_data_count: int):
        logger.trace("TrackerStatsSql: set_http_data_stats")
        self._enforce_acceptable_count_values(total_files_count)
        self._enforce_acceptable_count_values(files_with_http_data_count)
        sql = "insert into stats_http_data_stats (total_files_count,files_with_http_data_count,last_update_timestamp) values (?,?,?)"
        timestamp = arrow.now().datetime
        sql_data = (total_files_count, files_with_http_data_count, timestamp,)
        self.cur.execute(sql, sql_data)
        self.con.commit()
        self.zabbix.send_int_to_zabbix("objects_with_http_data_count", files_with_http_data_count)

    def get_http_data_stats(self) -> tuple:
        logger.trace("TrackerStatsSql: get_http_data_stats")
        sql = "select total_files_count, files_with_http_data_count from stats_http_data_stats order by id desc limit 1"
        self.cur.execute(sql)
        enrichment_stats: tuple = self.cur.fetchone()
        return enrichment_stats


    def set_enrichment_stats(self, total_files:int, enriched_files:int):
        logger.trace("TrackerStatsSql: set_enrichment_stats")
        self._enforce_acceptable_count_values(total_files)
        self._enforce_acceptable_count_values(enriched_files)
        sql = "insert into stats_enrichment_stats (total_files_count,enriched_files_count,last_update_timestamp) values (?,?,?)"
        timestamp = arrow.now().datetime
        sql_data = (total_files, enriched_files, timestamp,)
        self.cur.execute(sql, sql_data)
        self.con.commit()
        self.zabbix.send_int_to_zabbix("objects_enriched", enriched_files)

    def get_enrichment_stats(self):
        logger.trace("TrackerStatsSql: get_enrichment_stats")
        sql = "select total_files_count, enriched_files_count from stats_enrichment_stats order by id desc limit 1"
        self.cur.execute(sql)
        enrichment_stats: tuple = self.cur.fetchone()
        return enrichment_stats

    def get_last_objects_update(self) -> datetime.datetime:
        logger.trace("TrackerStatsSql: get_last_objects_update")
        sql = "select last_update_timestamp from stats_last_objects_update order by id desc limit 1"
        self.cur.execute(sql)
        last_objects_update = self.cur.fetchone()[0]
        return arrow.get(last_objects_update)

    def set_last_objects_update(self):
        """Save the last time an object was updated.

        To avoid wasting resources, write to database not more often than once per second.
        """
        logger.trace("TrackerStatsSql: set_last_objects_update")
        timestamp = arrow.now().datetime
        # throttle updates to be not more often than once per minute
        if self.last_objects_update_timestamp is None:
            self.last_objects_update_timestamp = timestamp
        else:
            delta = timestamp - self.last_objects_update_timestamp
            if delta.total_seconds() < 60:
                # throttle and do nothing
                return
        sql = "insert into stats_last_objects_update (last_update_timestamp) values (?)"
        sql_data = (timestamp,)
        self.cur.execute(sql, sql_data)
        self.con.commit()
