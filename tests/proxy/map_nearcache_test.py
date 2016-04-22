from hazelcast.config import NearCacheConfig
from tests.base import SingleMemberTestCase
from tests.util import random_string


class MapTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        near_cache_config = NearCacheConfig(random_string())
        # near_cache_config.time_to_live_seconds = 1000
        # near_cache_config.max_idle_seconds = 1000
        config.add_near_cache_config(near_cache_config)
        return super(MapTest, cls).configure_client(config)

    def setUp(self):
        name = self.client.config.near_cache_configs.values()[0].name
        self.map = self.client.get_map(name).blocking()

    def tearDown(self):
        self.map.destroy()

    def test_put_get(self):
        key = "key"
        value = "value"
        self.map.put(key, value)
        value2 = self.map.get(key)
        value3 = self.map.get(key)
        self.assertEqual(value, value2)
        self.assertEqual(value, value3)
        self.assertEqual(1, self.map._near_cache._cache_hit)
        self.assertEqual(1, self.map._near_cache._cache_miss)

    def test_put_get_remove(self):
        key = "key"
        value = "value"
        self.map.put(key, value)
        value2 = self.map.get(key)
        value3 = self.map.get(key)
        self.map.remove(key)
        self.assertEqual(value, value2)
        self.assertEqual(value, value3)
        self.assertEqual(1, self.map._near_cache._cache_hit)
        self.assertEqual(1, self.map._near_cache._cache_miss)
        self.assertEqual(0, len(self.map._near_cache))

    def _fill_map(self, count=10):
        map = {"key-%d" % x: "value-%d" % x for x in xrange(0, count)}
        for k, v in map.iteritems():
            self.map.put(k, v)
        return map
