import datetime
import pickle
import sqlite3
from pathlib import Path
from sys import breakpointhook
from unittest import result

import arrow
import portalocker
from loguru import logger

import lib.settings
import lib.zabbix


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
        logger.trace("TrackerStatsSql: __init__")
    
    def set_last_rss_update(self, timestamp: datetime.datetime):
        logger.trace("TrackerStatsSql: set_last_rss_update")
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
        sql = "insert into stats_rss_files_count (rss_files_count, last_update_timestamp) values (?,?)"
        timestamp = arrow.now().datetime
        sql_data = (count, timestamp,)
        self.cur.execute(sql, sql_data)
        self.con.commit()

    def get_rss_files_count(self) -> int:
        logger.trace("TrackerStatsSql: get_rss_files_count")
        sql = "select rss_files_count from stats_rss_files_count order by id desc limit 1"
        self.cur.execute(sql)
        rss_files_count = self.cur.fetchone()[0]
        return rss_files_count

    def set_objects_files_count(self, category: str, count: int):
        logger.trace("TrackerStatsSql: set_objects_files_count")
        sql = "insert into stats_objects_files_count (objects_files_count,category,last_update_timestamp) values (?,?,?)"
        timestamp = arrow.now().datetime
        sql_data = (count, category, timestamp,)
        self.cur.execute(sql, sql_data)
        self.con.commit()

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

    def set_http_data_stats(self, total_files_count, files_with_http_data_count):
        logger.trace("TrackerStatsSql: set_http_data_stats")
        sql = "insert into stats_http_data_stats (total_files_count,files_with_http_data_count,last_update_timestamp) values (?,?,?)"
        timestamp = arrow.now().datetime
        sql_data = (total_files_count, files_with_http_data_count, timestamp,)
        self.cur.execute(sql, sql_data)
        self.con.commit()
    
    def get_http_data_stats(self) -> tuple:
        logger.trace("TrackerStatsSql: get_http_data_stats")
        sql = "select total_files_count, files_with_http_data_count from stats_http_data_stats order by id desc limit 1"
        self.cur.execute(sql)
        enrichment_stats: tuple = self.cur.fetchone()
        return enrichment_stats


    def set_enrichment_stats(self, total_files:int, enriched_files:int):
        logger.trace("TrackerStatsSql: set_enrichment_stats")
        sql = "insert into stats_enrichment_stats (total_files_count,enriched_files_count,last_update_timestamp) values (?,?,?)"
        timestamp = arrow.now().datetime
        sql_data = (total_files, enriched_files, timestamp,)
        self.cur.execute(sql, sql_data)
        self.con.commit()
    
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