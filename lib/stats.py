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
        return arrow.get(last_rss_update).datetime

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

    def set_http_data_stats(self, total_files, files_with_http_data):
        logger.trace("TrackerStatsSql: set_http_data_stats")
    
    def get_http_data_stats(self) -> tuple:
        logger.trace("TrackerStatsSql: get_http_data_stats")


    def set_enrichment_stats(self, total_files, files_enriched):
        logger.trace("TrackerStatsSql: set_enrichment_stats")
    def get_enrichment_stats(self):
        logger.trace("TrackerStatsSql: get_enrichment_stats")
    def get_last_objects_update(self) -> datetime.datetime:
        logger.trace("TrackerStatsSql: get_last_objects_update")
    def set_last_objects_update(self):
        logger.trace("TrackerStatsSql: set_last_objects_update")
class TrackerStats:
    def __init__(self, settings: lib.settings.Settings):
        self.settings = settings
        self.data: StatsData = self.load()
        self.data.categories = list(self.settings.tracking_list.keys())
        self.zabbix = lib.zabbix.Zabbix(settings)
        self.lock_file = self.settings.stats_lock_file

    # def __del__(self):
    #   with logger.contextualize(task="Stats->Destructor"):
    #      logger.trace("Stats destructing, saving...")
    # self.save()

    def set_last_rss_update(self, timestamp: datetime.datetime):
        self.data.last_rss_update = timestamp
        self.save()
    
    def get_last_rss_update(self) -> datetime.datetime:
        return self.data.last_rss_update

    def set_rss_files_count(self, count: int):
        self.data.rss_files_count = count
        self.save()
    
    def get_rss_files_count(self) -> int:
        return self.data.rss_files_count

    def set_objects_files_count(self, category: str, count: int):
        self.data.objects_files_count[category] = count
        self.save()
    
    def get_objects_files_count(self, category: str) -> int:
        return self.data.objects_files_count[category]

    def set_http_data_stats(self, total_files, files_with_http_data):
        self.data.http_data = total_files, files_with_http_data
        self.save()
    
    def get_http_data_stats(self) -> tuple:
        return self.data.http_data


    def set_enrichment_stats(self, total_files, files_enriched):
        self.data.enrichment_data = total_files, files_enriched
        self.save()
    
    def get_enrichment_stats(self):
        return self.data.enrichment_data

    def get_last_objects_update(self) -> datetime.datetime:
        return self.data.last_objects_update

    def set_last_objects_update(self):
        """Update the last objects update time
        
        To avoid having to update it too frequently, 
        we only update it if it has been over a second since previous update
        """
        now = arrow.now()
        logger.trace(f"set_last_object_update() called at: {now}")
        if self.data.last_objects_update is None:
            # it is a new stats database that has not been updated before
            self.data.last_objects_update = now
            self.save()
        elif (now - self.data.last_objects_update).total_seconds() > 1:
            # avoid saving the file too often,
            # hardcoding a 3 second delta here
            self.data.last_objects_update = now
            self.save()
            logger.trace(f"Last object update set to: {now}")
        else:
            logger.trace("Last object update not necessary")

    #def gen_stats(self, object_store, rss_store):
    #    with logger.contextualize(task="Stats->Gen stats"):
    #       logger.trace("Generating stats")
    #      self.object_store = object_store
    #     self.rss_store = rss_store
    #    self.data.objects_count = object_store.get_classified_count()
    #   self.data.rss_files_count = rss_store.get_classified_count()

    def _get_stats_file(self):
        with logger.contextualize(task="Stats"):
            stats_file = Path(f"{self.settings.cache_dir}/stats.db")
            logger.trace(f"Returning stats file {stats_file}")
            return stats_file
    
    def _update_zabbix(self):

        with logger.contextualize(task="Stats->_update_zabbix"):
            logger.trace("Now updating zabbix")
            metrics = []
            metrics.append(
                self.zabbix.get_zabbix_metric(
                    "rss_files_count", self.data.rss_files_count
                )
            )

            stuff_to_update = ["house", "car", "dog", "apartment"]
            for type_of_stuff in stuff_to_update:
                if type_of_stuff in self.data.objects_files_count.keys():
                    metrics.append(
                        self.zabbix.get_zabbix_metric(
                            f"objects_{type_of_stuff}_count",
                            self.data.objects_files_count[type_of_stuff],
                        )
                    )

            if "total" in self.data.objects_files_count.keys():
                metrics.append(
                    self.zabbix.get_zabbix_metric(
                        "objects_count_total", self.data.objects_files_count["total"]
                    )
                )

            metrics.append(
                self.zabbix.get_zabbix_metric(
                    "objects_with_http_data_count", self.data.http_data[1]
                )
            )
            metrics.append(
                self.zabbix.get_zabbix_metric(
                    "objects_enriched", self.data.enrichment_data[1]
                )
            )
            result = self.zabbix.send_zabbix_metrics(metrics)
            logger.debug(f"Zabbix result: {result}")

    def save(self):
        with portalocker.Lock(self.lock_file) as lock:
            with logger.contextualize(task="Stats->Save"):
                logger.trace("Saving stats to pickle file")
                fh = open(self._get_stats_file(), "wb")
                pickle.dump(self.data, fh)
                logger.trace("Saved stats to pickle file")
                fh.close()
            
            self._update_zabbix()


    def load(self):
        with logger.contextualize(task="Stats->Load"):
            logger.trace("Loaded stats from pickle file")
            stats_file = self._get_stats_file()
            if not stats_file.exists():
                logger.warning("Stats file did not exist, using empty stats")
                return (
                    StatsData()
                )  # return a fresh instance in case nothing exists on disk
            else:
                fh = open(stats_file, "rb")
                return pickle.load(fh)