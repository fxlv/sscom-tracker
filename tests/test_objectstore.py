import arrow
import pytest

import lib.objectstore
import lib.settings
import lib.settings
import lib.helpers
import lib.datastructures


@pytest.fixture
def test_settings():
    settings = lib.settings.Settings("settings.test.json")
    return settings


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

    def test_init(self, test_settings):
        obj_store = lib.objectstore.ObjectStoreFiles(test_settings)
        assert isinstance(obj_store, lib.objectstore.ObjectStoreFiles)

    def test_load_non_existant_classified(self, test_settings):
        obj_store = lib.objectstore.ObjectStoreFiles(test_settings)
        classified = create_random_apartment_classified()
        loaded_classified = obj_store.get_classified(classified)
        # such classified does not exist yet, therefore we expect None
        assert loaded_classified is None

    def test_write_and_load(self, test_settings):
        obj_store = lib.objectstore.ObjectStoreFiles(test_settings)
        classified = create_random_apartment_classified()
        loaded_classified = obj_store.get_classified(classified)
        # such classified does not exist yet, therefore we expect None
        assert loaded_classified is None
        # save the classified and then read it back from storage
        assert obj_store.write_classified(classified) is True
        loaded_classified = obj_store.get_classified(classified)
        assert isinstance(loaded_classified, lib.datastructures.Classified)

    def test_load_all_classifieds_returns_a_list_of_classifieds(self, test_settings):
        obj_store = lib.objectstore.ObjectStoreFiles(test_settings)
        all_classifieds = obj_store.get_all_classifieds()
        assert isinstance(all_classifieds, list)
        one_classified = all_classifieds[0]
        assert isinstance(one_classified, lib.datastructures.Classified)

    def test_update_classified(self, test_settings):
        """Create, save, change, save, load and verify."""
        classified = create_random_apartment_classified()
        obj_store = lib.objectstore.ObjectStoreFiles(test_settings)
        obj_store.write_classified(classified)
        classified.title = "New title"
        obj_store.update_classified(classified)
        loaded_classified = obj_store.get_classified(classified)
        assert loaded_classified.title == classified.title
