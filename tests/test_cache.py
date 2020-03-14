import lib.cache
import lib.datastructures
import pytest
import os

@pytest.fixture
def local_cache():
    settings = {"local_cache":"test.cache.db"}
    cache = lib.cache.Cache(settings)
    return cache


def test_initialize_cache(local_cache):
    cache = local_cache
    assert isinstance(cache, lib.cache.Cache)


def test_add(local_cache: lib.cache.Cache):
    test_object = "Test item 1"
    local_cache.add(test_object)
    assert local_cache.is_known(test_object)


def test_save(local_cache):
    """Test writing, saving and reading back from cache."""
    test_cache_name = "test.cache2.db"
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
    test_cache_name = "test.cache3.db"
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
    # delete object, by decrementing its reference counter
    del(cache)
    time2 = os.path.getmtime(test_cache_name)
    # now compare timestamps before and after destruction
    assert time2 > time1


def test_ClassifiedCache():
    test_cache_name = "test.ClassifiedCache.db"
    settings = {"local_cache": test_cache_name}
    # ensure clean environment, delete cache file if it exists
    if os.path.exists(test_cache_name):
        os.unlink(test_cache_name)
    # make sure file does not exist before continuing
    assert os.path.exists(test_cache_name) is False
    cache = lib.cache.ClassifiedCache(settings)
    # make sure new cache has been initialized and is empty
    assert len(cache.cache) == 0
    # test adding and checking for a presence of an object
    test_object = lib.datastructures.Classified()
    test_object.title = "Something"
    test_object.id = 1
    test_object.street = "Some street"
    cache.add(test_object)
    # first assert will fail as the hash has not yet been added to cache
    assert not cache.is_known(test_object)
    # second assert will succeed as the hash has been added to the cache
    assert cache.is_known(test_object)


