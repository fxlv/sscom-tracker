import pytest

import lib.objectstore
import lib.settings
import lib.settings


class TestObjectstore:
    def test_init_fails_if_no_settings_provided(self):
        with pytest.raises(TypeError):
            obj_store = lib.objectstore.ObjectStoreFiles()

    def test_init(self):
        settings = lib.settings.Settings("settings.test.json")
        obj_store = lib.objectstore.ObjectStoreFiles(settings)
        assert isinstance(obj_store, lib.objectstore.ObjectStoreFiles)
