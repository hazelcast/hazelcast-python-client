import os

from tests.hzrc.ttypes import Lang

from hazelcast.config import NearCacheConfig
from tests.base import SingleMemberTestCase
from tests.util import random_string
from hazelcast.six.moves import range
from hazelcast import six


class MapTest(SingleMemberTestCase):
    @classmethod
    def configure_cluster(cls):
        path = os.path.abspath(__file__)
        dir_path = os.path.dirname(path)
        with open(os.path.join(dir_path, "hazelcast.xml")) as f:
            return f.read()

    @classmethod
    def configure_client(cls, config):
        near_cache_config = NearCacheConfig(random_string())
        # near_cache_config.time_to_live_seconds = 1000
        # near_cache_config.max_idle_seconds = 1000
        config.add_near_cache_config(near_cache_config)
        return super(MapTest, cls).configure_client(config)

    def setUp(self):
        name = list(self.client.config.near_cache_configs.values())[0].name
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
        self.assertEqual(1, self.map._near_cache._hits)
        self.assertEqual(1, self.map._near_cache._misses)

    def test_put_get_remove(self):
        key = "key"
        value = "value"
        self.map.put(key, value)
        value2 = self.map.get(key)
        value3 = self.map.get(key)
        self.map.remove(key)
        self.assertEqual(value, value2)
        self.assertEqual(value, value3)
        self.assertEqual(1, self.map._near_cache._hits)
        self.assertEqual(1, self.map._near_cache._misses)
        self.assertEqual(0, len(self.map._near_cache))

    def test_invalidate_single_key(self):
        self._fill_map_and_near_cache(10)
        initial_cache_size = len(self.map._near_cache)
        script = """map = instance_0.getMap("{}");map.remove("key-5")""".format(self.map.name)
        response = self.rc.executeOnController(self.cluster.id, script, Lang.PYTHON)
        self.assertTrue(response.success)
        self.assertEqual(initial_cache_size, 10)

        def assertion():
            self.assertEqual(len(self.map._near_cache), 9)

        self.assertTrueEventually(assertion)

    def test_invalidate_nonexist_key(self):
        self._fill_map_and_near_cache(10)
        initial_cache_size = len(self.map._near_cache)
        script = """map = instance_0.getMap("{}");map.put("key-99","x");map.put("key-NonExist","x");map.remove("key-NonExist")"""\
            .format(self.map.name)
        response = self.rc.executeOnController(self.cluster.id, script, Lang.PYTHON)
        self.assertTrue(response.success)
        self.assertEqual(initial_cache_size, 10)

        def assertion():
            self.assertEqual(self.map.size(), 11)
            self.assertEqual(len(self.map._near_cache), 10)

        self.assertTrueEventually(assertion)

    def test_invalidate_multiple_keys(self):
        self._fill_map_and_near_cache(10)
        initial_cache_size = len(self.map._near_cache)
        script = """map = instance_0.getMap("{}");map.clear()""".format(self.map.name)
        response = self.rc.executeOnController(self.cluster.id, script, Lang.PYTHON)
        self.assertTrue(response.success)
        self.assertEqual(initial_cache_size, 10)

        def assertion():
            self.assertEqual(len(self.map._near_cache), 0)

        self.assertTrueEventually(assertion)

    def _fill_map_and_near_cache(self, count=10):
        fill_content = {"key-%d" % x: "value-%d" % x for x in range(0, count)}
        for k, v in six.iteritems(fill_content):
            self.map.put(k, v)
        for k, v in six.iteritems(fill_content):
            self.map.get(k)
        return fill_content
