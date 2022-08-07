from pyzabbix import ZabbixMetric, ZabbixSender

import lib.settings

from loguru import logger


class Zabbix:
    def __init__(self, settings: lib.settings.Settings):
        self.settings = settings

    def get_zabbix_metric(self, key: str, value):
        return ZabbixMetric(self.settings.zabbix_trap_host, key, value)

    def send_zabbix_metrics(self, metrics: list):
        zs = ZabbixSender(self.settings.zabbix_server, self.settings.zabbix_port)
        result = zs.send(metrics)
        return result

    def send_zabbix_metric(self, metric):
        return self.send_zabbix_metrics([metric])
    
    def send_int_to_zabbix(self, key: str, value: int): 
        metric = self.get_zabbix_metric(key, value)
        logger.trace(f"Sending zabbix metric {metric}")
        self.send_zabbix_metric(metric)
