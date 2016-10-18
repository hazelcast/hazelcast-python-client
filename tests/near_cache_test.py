import unittest
from time import sleep

from hazelcast import SerializationConfig
from hazelcast.config import NearCacheConfig
from hazelcast.near_cache import *
from hazelcast.serialization import SerializationServiceV1
from tests.util import random_string


class NearCacheTestCase(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
        logging.getLogger().setLevel(logging.DEBUG)

        self.service = SerializationServiceV1(serialization_config=SerializationConfig())

    def tearDown(self):
        self.service.destroy()

    def test_near_cache_config(self):
        config = NearCacheConfig(random_string())
        with self.assertRaises(ValueError):
            config.in_memory_format = 100

        with self.assertRaises(ValueError):
            config.eviction_policy = 100

        with self.assertRaises(ValueError):
            config.time_to_live_seconds = -1

        with self.assertRaises(ValueError):
            config.max_idle_seconds = -1

        with self.assertRaises(ValueError):
            config.eviction_max_size = 0

    def test_DataRecord_expire_time(self):
        now = current_time()
        print int(now), now
        data_rec = DataRecord("key", "value", create_time=now, ttl_seconds=1)
        sleep(2)
        self.assertTrue(data_rec.is_expired(max_idle_seconds=1000))

    def test_DataRecord_max_idle_seconds(self):
        now = current_time()
        data_rec = DataRecord("key", "value", create_time=now, ttl_seconds=1000)
        sleep(2)
        self.assertTrue(data_rec.is_expired(max_idle_seconds=1))

    def test_put_get_data(self):
        near_cache = self.create_near_cache(self.service, IN_MEMORY_FORMAT.BINARY, 1000, 1000, EVICTION_POLICY.LRU, 1000)
        key_data = self.service.to_data("key")
        near_cache[key_data] = "value"
        self.assertEqual("value", near_cache[key_data])

    def test_put_get(self):
        near_cache = self.create_near_cache(self.service, IN_MEMORY_FORMAT.OBJECT, 1000, 1000, EVICTION_POLICY.LRU, 1000)
        for i in xrange(0, 10000):
            key = "key-{}".format(i)
            value = "value-{}".format(i)
            near_cache[key] = value
            self.assertEqual(value, near_cache[key])
            self.assertEqual("value-0", near_cache["key-0"])  # prevent its eviction
            self.assertGreaterEqual(near_cache.eviction_max_size * 1.1, near_cache.__len__())

    def test_expiry_time(self):
        near_cache = self.create_near_cache(self.service, IN_MEMORY_FORMAT.OBJECT, 1, 1000, EVICTION_POLICY.LRU, 1000)
        for i in xrange(0, 1000):
            key = "key-{}".format(i)
            value = "value-{}".format(i)
            near_cache[key] = value
        sleep(2)
        for i in xrange(1001, 1010):
            key = "key-{}".format(i)
            value = "value-{}".format(i)
            near_cache[key] = value
        evict, expire = near_cache.get_statistics()
        self.assertLess(evict, 2)
        self.assertGreater(expire, 8)

    def test_max_idle_time(self):
        near_cache = self.create_near_cache(self.service, IN_MEMORY_FORMAT.OBJECT, 1000, 2, EVICTION_POLICY.LRU, 1000)
        for i in xrange(0, 1000):
            key = "key-{}".format(i)
            value = "value-{}".format(i)
            near_cache[key] = value
        sleep(3)
        near_cache["key"] = "value"
        evict, expire = near_cache.get_statistics()
        self.assertEqual(evict, 0)
        self.assertEqual(expire, near_cache.eviction_sampling_count)

    def test_LRU_time(self):
        near_cache = self.create_near_cache(self.service, IN_MEMORY_FORMAT.OBJECT, 1000, 1000, EVICTION_POLICY.LRU, 10000, 16, 16)
        for i in xrange(0, 10000):
            key = "key-{}".format(i)
            value = "value-{}".format(i)
            near_cache[key] = value
        self.assertEqual("value-0", near_cache["key-0"])
        for i in xrange(10001, 20000):
            key = "key-{}".format(i)
            value = "value-{}".format(i)
            near_cache[key] = value
        evict, expire = near_cache.get_statistics()
        self.assertEqual(expire, 0)
        self.assertLess(evict, 10000)

    def test_LRU_time_with_update(self):
        near_cache = self.create_near_cache(self.service, IN_MEMORY_FORMAT.OBJECT, 1000, 1000, EVICTION_POLICY.LRU, 10, 10, 10)
        for i in xrange(0, 10):
            key = "key-{}".format(i)
            value = "value-{}".format(i)
            near_cache[key] = value
        for i in xrange(0, 9):
            key = "key-{}".format(i)
            value = "value-{}".format(i)
            self.assertEqual(value, near_cache[key])
        near_cache["key-10"] = "value-10"
        with self.assertRaises(KeyError):
            val = near_cache["key-9"]

    def test_LFU_time(self):
        near_cache = self.create_near_cache(self.service, IN_MEMORY_FORMAT.BINARY, 1000, 1000, EVICTION_POLICY.LFU, 1000)
        for i in xrange(0, 1000):
            key = "key-{}".format(i)
            value = "value-{}".format(i)
            near_cache[key] = value
            for j in xrange(0, i + 1):
                v = near_cache[key]
        for i in xrange(1001, 2000):
            key = "key-{}".format(i)
            value = "value-{}".format(i)
            near_cache[key] = value
        evict, expire = near_cache.get_statistics()
        self.assertEqual(expire, 0)
        self.assertLess(evict, 1000)

    def test_RANDOM_time(self):
        near_cache = self.create_near_cache(self.service, IN_MEMORY_FORMAT.BINARY, 1000, 1000, EVICTION_POLICY.LFU, 1000)
        for i in xrange(0, 2000):
            key = "key-{}".format(i)
            value = "value-{}".format(i)
            near_cache[key] = value
        evict, expire = near_cache.get_statistics()
        self.assertEqual(expire, 0)
        self.assertGreaterEqual(evict, 1000)

    def create_near_cache(self, service, im_format, ttl, max_idle, policy, max_size, eviction_sampling_count=None,
                          eviction_sampling_pool_size=None):
        return NearCache(service, im_format, ttl, max_idle, True, policy, max_size, eviction_sampling_count,
                         eviction_sampling_pool_size)
