import logging
from typing import Tuple, List


class Filter:
    def __init__(self, retriever, cache, settings):
        self.retriever = retriever
        self.cache = cache
        self.settings = settings
        self.tracking_list = self.settings["tracking_list"]

    def filter_tracking_list(self):
        results = {}
        for classified_type in self.tracking_list:
            results[classified_type] = {}
            url = self.tracking_list[classified_type]["url"]
            results_new, results_old = self.filter_by_type(classified_type, url)
            results[classified_type]["new"] = results_new
            results[classified_type]["old"] = results_old
        return results

    def filter_by_type(self, classified_type: str, url: str) -> Tuple[List, List]:
        logging.info("Looking for type: %s using URL: %s", classified_type, url)
        k = self.retriever.get_ss_data_from_cache(url)
        ad_list = self.retriever.get_ad_list(k, classified_type)
        results_old = []
        results_new = []
        for a in ad_list:

            if self.cache.is_known(a):
                logging.info("OLD: %s [%s]", a, a.get_hash())
                results_old.append(a)
            else:
                self.cache.add(a)
                logging.info("NEW: %s [%s]", a, a.get_hash())

                if classified_type == "apartment":
                    if (
                        int(a.rooms)
                        >= self.tracking_list[classified_type]["filter_room_count"]
                    ):
                        logging.debug("NEW Apartment matching filtering criteria found")
                        results_new.append(a)
                elif classified_type == "house":
                    logging.info("NEW House found: %s", str(a))
                    results_new.append(a)
                elif classified_type == "dog":
                    logging.info("NEW Dog found: %s", str(a))
                    results_new.append(a)
                else:
                    logging.info("Not enough rooms (%s)", a.rooms)
        return results_new, results_old
