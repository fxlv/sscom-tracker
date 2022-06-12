import json

import os


class Settings:
    def __init__(self, settings_file_name: str = "settings.json"):
        self.settings_file_name = self._get_settings_file()
        if not self.settings_file_name:
            raise RuntimeError("Settings file is missing")
        self._load_settings_from_file()
        # these are the supported settings attributes
        self.pushover_enabled: bool = None
        self.pushover_user_key: str = None
        self.pushover_api_token: str = None
        self.local_cache: str = None
        self.data_cache: str = None
        self.zabbix_enabled: bool = False
        self.zabbix_server: str = None
        self.zabbix_trap_host: str = None
        self.zabbix_port: int = None
        self.log_dir: str = None
        self.lock_file: str = "tracker.lock"
        self.stats_lock_file: str = "stats.lock"
        self.log_file_name = "tracker.log"  # default, but can be overriden
        self.log_rotation: str = None
        self.cache_dir: str = None
        self.object_cache_dir: str = None
        self.cache_validity_time: int = None
        self.tracking_list: dict = None
        self.sqlite_db: str = None
        self.storage_type: str = "files"

        self._parse_settings()

    def _get_settings_file(self):
        """Search for settings file in the know locations."""
        settings_locations = [
            "settings.json",
            "settings.test.json",
            "/config/settings.json",
        ]
        for location in settings_locations:
            if os.path.exists(location):
                return location
        return None

    def _get_setting(self, setting_key):
        setting_value = None
        if setting_key in self._settings_dict.keys():
            setting_value = self._settings_dict[setting_key]
        return setting_value

    def _parse_settings(self):
        self.pushover_enabled = self._get_setting("pushover_enabled")
        self.pushover_api_token = self._get_setting("pushover_api_token")
        self.pushover_user_key = self._get_setting("pushover_user_key")

        self.local_cache = self._get_setting("local_cache")
        self.cache_dir = self._get_setting("cache_dir")
        self.zabbix_server = self._get_setting("zabbix_server")
        self.lock_file = self._get_setting("lock_file")
        self.stats_lock_file = self._get_setting("stats_lock_file")
        self.zabbix_trap_host = self._get_setting("zabbix_trap_host")
        self.zabbix_enabled = self._get_setting("zabbix_enabled")
        self.zabbix_port = int(self._get_setting("zabbix_port"))
        self.object_cache_dir = self._get_setting("object_cache_dir")
        self.log_dir = self._get_setting("log_dir")
        self.log_rotation = self._get_setting("log_rotation")
        self.data_cache = self._get_setting("data_cache")
        try:
            self.cache_validity_time = int(self._get_setting("cache_validity_time"))
        except TypeError:
            raise TypeError(
                "Cache validity time in settings is either missing or invalid."
            )
        self.tracking_list = self._get_setting("tracking_list")
        self.sqlite_db = self._get_setting("sqlite_db")
        self.storage_type = self._get_setting("storage_type")

    def _load_settings_from_file(self):
        with open(self.settings_file_name) as settings_file_handle:
            self._settings_dict = json.load(settings_file_handle)


class TestSettings(Settings):
    """Dummy Settings class meant for Unittesting purposes."""

    def __init__(self, local_cache=None):
        self.pushover_enabled: bool = None
        self.pushover_user_key: str = None
        self.pushover_api_token: str = None
        self.local_cache: str = local_cache
        self.data_cache: str = None
        self.cache_validity_time: int = None
        self.cache_dir: str = None
        self.object_cache_dir: str = None
        self.tracking_list: dict = None
        self.log_dir: str = "test_log"
        self.log_file_name = "tracker.log"  # default, but can be overriden
        self.log_rotation: str = "15 KB"
        self.storage_type: str = "files"
