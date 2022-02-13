import arrow
import pytest

import lib.objectstore
import lib.settings
import lib.settings
import lib.helpers
import lib.datastructures
from loguru import logger

from lib.store import ObjectStore


@pytest.fixture
def test_settings():
    settings = lib.settings.Settings("settings.test.json")
    return settings


@pytest.fixture
def object_store(test_settings):
    obj_store = lib.objectstore.get_object_store("sqlite")(test_settings)
    return obj_store


def create_random_apartment_classified():
    random_short_hash = lib.helpers.get_random_short_hash()
    random_title = f"Some apartment_{random_short_hash}"
    classified = lib.datastructures.Apartment(random_title, "Some street name")
    classified.published = arrow.now()
    classified.price = "100"
    classified.done()
    return classified


class TestObjectstore:
    def test_init_fails_if_no_settings_provided(self):
        with pytest.raises(TypeError):
            obj_store = lib.objectstore.ObjectStoreFiles()

    def test_init(self, object_store):
        assert isinstance(object_store, lib.objectstore.ObjectStoreSqlite)

    def test_load_non_existant_classified(self, object_store):
        classified = create_random_apartment_classified()
        loaded_classified = object_store.get_classified(classified)
        # such classified does not exist yet, therefore we expect None
        assert loaded_classified is None

    def test_write_and_load(self, object_store: ObjectStore):
        classified = create_random_apartment_classified()
        loaded_classified = object_store.get_classified(classified)
        # such classified does not exist yet, therefore we expect None
        assert loaded_classified is None
        # save the classified and then read it back from storage
        assert object_store.write_classified(classified) is True
        loaded_classified = object_store.get_classified(classified)
        assert isinstance(loaded_classified, lib.datastructures.Classified)

    def test_load_all_classifieds_returns_a_list_of_classifieds(self, object_store: ObjectStore):
        all_classifieds = object_store.get_all_classifieds("apartment")
        #pytest.set_trace()
        assert isinstance(all_classifieds, list)
        one_classified = all_classifieds[0]
        assert isinstance(one_classified, lib.datastructures.Classified)

    def test_update_classified(self, object_store):
        """Create, save, change, save, load and verify."""
        classified = create_random_apartment_classified()
        object_store.write_classified(classified)
        classified.title = "New title"
        object_store.update_classified(classified)
        loaded_classified = object_store.get_classified(classified)
        assert loaded_classified.title == classified.title

    def test_get_classified_by_hash(self, object_store):
        classified = create_random_apartment_classified()
        object_store.write_classified(classified)
        loaded_classified = object_store.get_classified_by_category_hash(
            classified.category, classified.hash
        )
        assert classified.hash == loaded_classified.hash
