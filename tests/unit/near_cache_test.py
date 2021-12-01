import unittest
from time import sleep

from hazelcast.config import _Config
from hazelcast.near_cache import *
from hazelcast.serialization import SerializationServiceV1


class NearCacheTestCase(unittest.TestCase):
    def setUp(self):
        self.service = SerializationServiceV1(_Config())

    def tearDown(self):
        self.service.destroy()

    def test_DataRecord_expire_time(self):
        now = current_time()
        data_rec = DataRecord("key", "value", create_time=now, ttl_seconds=0.05)
        sleep(0.1)
        self.assertTrue(data_rec.is_expired(max_idle_seconds=1000))

    def test_DataRecord_max_idle_seconds(self):
        now = current_time()
        data_rec = DataRecord("key", "value", create_time=now, ttl_seconds=1000)
        sleep(0.1)
        self.assertTrue(data_rec.is_expired(max_idle_seconds=0.05))

    def test_put_get_data(self):
        near_cache = self.create_near_cache(
            self.service, InMemoryFormat.BINARY, 1000, 1000, EvictionPolicy.LRU, 1000
        )
        key_data = self.service.to_data("key")
        near_cache[key_data] = "value"
        self.assertEqual("value", near_cache[key_data])

    def test_put_get(self):
        near_cache = self.create_near_cache(
            self.service, InMemoryFormat.OBJECT, 100, 100, EvictionPolicy.LRU, 100
        )
        for i in range(0, 120):
            key = "key-{}".format(i)
            value = "value-{}".format(i)
            near_cache[key] = value
            self.assertEqual(value, near_cache[key])
            self.assertEqual("value-0", near_cache["key-0"])  # prevent its eviction
            self.assertGreaterEqual(near_cache.eviction_max_size * 1.1, near_cache.__len__())

    def test_expiry_time(self):
        near_cache = self.create_near_cache(
            self.service, InMemoryFormat.OBJECT, 0.05, 100, EvictionPolicy.LRU, 100
        )
        for i in range(0, 100):
            key = "key-{}".format(i)
            value = "value-{}".format(i)
            near_cache[key] = value
        sleep(0.1)
        for i in range(101, 110):
            key = "key-{}".format(i)
            value = "value-{}".format(i)
            near_cache[key] = value
        stats = near_cache.get_statistics()
        evict, expire = stats["evictions"], stats["expirations"]
        self.assertLess(evict, 2)
        self.assertGreater(expire, 8)

    def test_max_idle_time(self):
        near_cache = self.create_near_cache(
            self.service, InMemoryFormat.OBJECT, 1000, 0.05, EvictionPolicy.LRU, 1000
        )
        for i in range(0, 1000):
            key = "key-{}".format(i)
            value = "value-{}".format(i)
            near_cache[key] = value
        sleep(0.1)
        near_cache["key"] = "value"
        stats = near_cache.get_statistics()
        evict, expire = stats["evictions"], stats["expirations"]
        self.assertEqual(evict, 0)
        self.assertEqual(expire, near_cache.eviction_sampling_count)

    def test_LRU_time(self):
        near_cache = self.create_near_cache(
            self.service, InMemoryFormat.OBJECT, 100, 100, EvictionPolicy.LRU, 1000, 16, 16
        )
        for i in range(0, 1000):
            key = "key-{}".format(i)
            value = "value-{}".format(i)
            near_cache[key] = value
        self.assertEqual("value-0", near_cache["key-0"])
        for i in range(1001, 2000):
            key = "key-{}".format(i)
            value = "value-{}".format(i)
            near_cache[key] = value
        stats = near_cache.get_statistics()
        evict, expire = stats["evictions"], stats["expirations"]
        self.assertEqual(expire, 0)
        self.assertLess(evict, 1000)

    def test_LRU_time_with_update(self):
        near_cache = self.create_near_cache(
            self.service, InMemoryFormat.OBJECT, 1000, 1000, EvictionPolicy.LRU, 10, 10, 10
        )
        for i in range(0, 10):
            key = "key-{}".format(i)
            value = "value-{}".format(i)
            near_cache[key] = value

        sleep(0.1)

        for i in range(0, 9):
            key = "key-{}".format(i)
            value = "value-{}".format(i)
            self.assertEqual(value, near_cache[key])

        sleep(0.1)
        near_cache["key-10"] = "value-10"
        with self.assertRaises(KeyError):
            val = near_cache["key-9"]

    def test_LFU_time(self):
        near_cache = self.create_near_cache(
            self.service, InMemoryFormat.BINARY, 1000, 1000, EvictionPolicy.LFU, 100
        )
        for i in range(0, 100):
            key = "key-{}".format(i)
            value = "value-{}".format(i)
            near_cache[key] = value
            for _ in range(0, i + 1):
                v = near_cache[key]

        for i in range(101, 200):
            key = "key-{}".format(i)
            value = "value-{}".format(i)
            near_cache[key] = value

        stats = near_cache.get_statistics()
        evict, expire = stats["evictions"], stats["expirations"]
        self.assertEqual(expire, 0)
        self.assertLess(evict, 100)

    def test_RANDOM_time(self):
        near_cache = self.create_near_cache(
            self.service, InMemoryFormat.BINARY, 1000, 1000, EvictionPolicy.LFU, 100
        )
        for i in range(0, 200):
            key = "key-{}".format(i)
            value = "value-{}".format(i)
            near_cache[key] = value
        stats = near_cache.get_statistics()
        evict, expire = stats["evictions"], stats["expirations"]
        self.assertEqual(expire, 0)
        self.assertGreaterEqual(evict, 100)

    def create_near_cache(
        self,
        service,
        im_format,
        ttl,
        max_idle,
        policy,
        max_size,
        eviction_sampling_count=None,
        eviction_sampling_pool_size=None,
    ):
        return NearCache(
            "default",
            service,
            im_format,
            ttl,
            max_idle,
            True,
            policy,
            max_size,
            eviction_sampling_count,
            eviction_sampling_pool_size,
        )
