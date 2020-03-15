import lib.cache
import lib.datastructures
import pytest
import os
import  lib.log
import logging
import datetime
import time
lib.log.set_up_logging(debug=True, log_file="tests_debug.log")

@pytest.fixture
def local_cache():
    settings = {"local_cache":"test.cache.db"}
    cache = lib.cache.Cache(settings)
    return cache

def test_initialize_cache(local_cache):
    cache = local_cache
    assert isinstance(cache, lib.cache.Cache)

def test_initialize_cache_with_overriden_cache_file():
    settings = {"local_cache":"test.cache.db"}
    test_cache_name = "test_cache_overriden.db"
    if os.path.exists(test_cache_name):
        logging.debug("Previous cache file {} exists. Deleting it.".format(test_cache_name))
        os.unlink(test_cache_name)
    assert os.path.exists(test_cache_name) is False
    cache = lib.cache.Cache(settings, test_cache_name)
    cache.save()
    assert os.path.exists(test_cache_name)
    assert isinstance(cache, lib.cache.Cache)

def test_add(local_cache: lib.cache.Cache):
    test_object = "Test item 1"
    local_cache.add(test_object)
    assert local_cache.is_known(test_object)

def test_save(local_cache):
    """Test writing, saving and reading back from cache."""
    test_cache_name = "test_cache2.db"
    settings = {"local_cache": test_cache_name}
    # ensure clean environment, delete cache file if it exists
    if os.path.exists(test_cache_name):
        os.unlink(test_cache_name)
    # make sure file does not exist before continuing
    assert os.path.exists(test_cache_name) is False
    cache = lib.cache.Cache(settings)
    # make sure new cache has been initialized and is empty
    assert len(cache.cache) == 0
    # add an item to cache
    test_object = "Test item 2"
    cache.add(test_object)
    cache.save()
    # now validate that file was written to disk
    assert os.path.exists(test_cache_name)
    # now load cache from disk and check consitency
    # by expecting to see one item in it
    cache2 = lib.cache.Cache(settings)
    assert len(cache.cache) == 1
    assert cache2.is_known(test_object)

def test_destructor():
    """Ensure cache is dumped to disk before destruction.

    Test this by comparing modification time
    before and after destruction.
    """
    test_cache_name = "test_cache3.db"
    settings = {"local_cache": test_cache_name}
    # ensure clean environment, delete cache file if it exists
    if os.path.exists(test_cache_name):
        os.unlink(test_cache_name)
    # make sure file does not exist before continuing
    assert os.path.exists(test_cache_name) is False
    cache = lib.cache.Cache(settings)
    # make sure new cache has been initialized and is empty
    assert len(cache.cache) == 0
    cache.save()
    time1 = os.path.getmtime(test_cache_name)
    time.sleep(0.1)
    # delete object, by decrementing its reference counter
    del(cache)
    time2 = os.path.getmtime(test_cache_name)
    # now compare timestamps before and after destruction
    assert time2 > time1

def test_DataCache():

    test_cache_name = "test_data_cache.db"
    settings = {
        "data_cache": test_cache_name,
        "cache_freshness": 300
    }

    # ensure clean environment, delete cache file if it exists
    if os.path.exists(test_cache_name):
        os.unlink(test_cache_name)
    # make sure file does not exist before continuing
    assert os.path.exists(test_cache_name) is False

    cache = lib.cache.DataCache(settings)
    # make sure new cache has been initialized and is empty
    # at this point it should only contain: {"data": {}, "last_update": None}
    assert len(cache.cache.keys()) == 2
    cache.save()
    # add stuff to cache
    cache.add("test_item", "something")
    # and test retrieval of stuff
    assert cache.get("test_item") == "something"
    assert cache.is_known("test_item")
    assert type(cache.get_timestamp()) == datetime.datetime
