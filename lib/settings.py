import json

import os


class Settings:
    def __init__(self, settings_file_name: str = "settings.json"):
        self.settings_file_name = settings_file_name
        if not self._settings_file_exists():
            raise RuntimeError("Settings file is missing")
        self._load_settings_from_file()
        # these are the supported settings attributes
        self.pushover_enabled: bool = None
        self.pushover_user_key: str = None
        self.pushover_api_token: str = None
        self.local_cache: str = None
        self.data_cache: str = None
        self.cache_validity_time: int = None
        self.tracking_list: dict = None

        self._parse_settings()

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
        self.data_cache = self._get_setting("data_cache")
        try:
            self.cache_validity_time = int(self._get_setting("cache_validity_time"))
        except TypeError:
            raise TypeError(
                "Cache validity time in settings is either missing or invalid."
            )
        self.tracking_list = self._get_setting("tracking_list")

    def _load_settings_from_file(self):
        with open(self.settings_file_name) as settings_file_handle:
            self._settings_dict = json.load(settings_file_handle)

    def _settings_file_exists(self):
        return os.path.exists(self.settings_file_name)


class TestSettings(Settings):
    """Dummy Settings class meant for Unittesting purposes."""

    def __init__(self, local_cache=None):
        self.pushover_enabled: bool = None
        self.pushover_user_key: str = None
        self.pushover_api_token: str = None
        self.local_cache: str = local_cache
        self.data_cache: str = None
        self.cache_validity_time: int = None
        self.tracking_list: dict = None
