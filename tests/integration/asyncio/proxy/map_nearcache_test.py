import asyncio
import os
import unittest

from hazelcast.config import ReconnectMode
from hazelcast.errors import ClientOfflineError
from hazelcast.lifecycle import LifecycleState
from hazelcast.predicate import true
from tests.hzrc.ttypes import Lang

from tests.integration.asyncio.base import SingleMemberTestCase, HazelcastTestCase
from tests.util import random_string, skip_if_client_version_older_than
from hazelcast.asyncio import HazelcastClient


class MapTest(SingleMemberTestCase):
    @classmethod
    def configure_cluster(cls):
        path = os.path.abspath(__file__)
        dir_path = os.path.dirname(path)
        with open(os.path.join(dir_path, "../../backward_compatible/proxy/hazelcast.xml")) as f:
            return f.read()

    @classmethod
    def configure_client(cls, config):
        cls.map_name = random_string()
        config["cluster_name"] = cls.cluster.id
        config["near_caches"] = {cls.map_name: {}}
        return config

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.map = await self.client.get_map(self.map_name)

    async def asyncTearDown(self):
        await self.map.destroy()
        await super().asyncTearDown()

    async def test_put_get(self):
        key = "key"
        value = "value"
        await self.map.put(key, value)
        value2 = await self.map.get(key)
        value3 = await self.map.get(key)
        self.assertEqual(value, value2)
        self.assertEqual(value, value3)
        self.assertEqual(1, self.map._near_cache._hits)
        self.assertEqual(1, self.map._near_cache._misses)

    async def test_put_get_remove(self):
        key = "key"
        value = "value"
        await self.map.put(key, value)
        value2 = await self.map.get(key)
        value3 = await self.map.get(key)
        await self.map.remove(key)
        self.assertEqual(value, value2)
        self.assertEqual(value, value3)
        self.assertEqual(1, self.map._near_cache._hits)
        self.assertEqual(1, self.map._near_cache._misses)
        self.assertEqual(0, len(self.map._near_cache))

    async def test_remove_all(self):
        skip_if_client_version_older_than(self, "5.6.0")

        await self.fill_map_and_near_cache(10)
        await self.map.remove_all(predicate=true())
        self.assertEqual(0, len(self.map._near_cache))

    async def test_invalidate_single_key(self):
        await self.fill_map_and_near_cache(10)
        initial_cache_size = len(self.map._near_cache)
        script = """map = instance_0.getMap("{}");map.remove("key-5")""".format(self.map.name)
        response = await asyncio.to_thread(
            self.rc.executeOnController, self.cluster.id, script, Lang.PYTHON
        )
        self.assertTrue(response.success)
        self.assertEqual(initial_cache_size, 10)

        def assertion():
            self.assertEqual(9, len(self.map._near_cache))

        await self.assertTrueEventually(assertion, timeout=30)

    async def test_invalidate_nonexist_key(self):
        await self.fill_map_and_near_cache(10)
        initial_cache_size = len(self.map._near_cache)
        script = (
            """
        var map = instance_0.getMap("%s");
        map.put("key-99","x");
        map.put("key-NonExist","x");
        map.remove("key-NonExist");"""
            % self.map.name
        )

        response = self.rc.executeOnController(self.cluster.id, script, Lang.JAVASCRIPT)
        self.assertTrue(response.success)
        self.assertEqual(initial_cache_size, 10)

        async def assertion():
            self.assertEqual(await self.map.size(), 11)
            self.assertEqual(len(self.map._near_cache), 10)

        await self.assertTrueEventually(assertion)

    async def test_invalidate_multiple_keys(self):
        await self.fill_map_and_near_cache(10)
        initial_cache_size = len(self.map._near_cache)
        script = """map = instance_0.getMap("{}");map.clear()""".format(self.map.name)
        response = await asyncio.to_thread(
            self.rc.executeOnController, self.cluster.id, script, Lang.PYTHON
        )
        self.assertTrue(response.success)
        self.assertEqual(initial_cache_size, 10)

        def assertion():
            self.assertEqual(0, len(self.map._near_cache))

        await self.assertTrueEventually(assertion, timeout=60)

    async def fill_map_and_near_cache(self, count=10):
        fill_content = {"key-%d" % x: "value-%d" % x for x in range(0, count)}
        for k, v in fill_content.items():
            await self.map.put(k, v)
        for k, v in fill_content.items():
            await self.map.get(k)
        return fill_content


ENTRY_COUNT = 100


class NonStopNearCacheTest(unittest.IsolatedAsyncioTestCase, HazelcastTestCase):
    async def asyncSetUp(self):
        rc = self.create_rc()
        cluster = self.create_cluster(rc, self.read_cluster_config())
        cluster.start_member()

        def event_collector():
            events = []

            def collector(e):
                if e == LifecycleState.DISCONNECTED:
                    events.append(e)

            collector.events = events
            return collector

        collector = event_collector()

        client = await HazelcastClient.create_and_start(
            cluster_name=cluster.id,
            reconnect_mode=ReconnectMode.ASYNC,
            near_caches={"map": {}},
            lifecycle_listeners=[collector],
        )

        map = await client.get_map("map")
        for i in range(ENTRY_COUNT):
            await map.put(i, i)

        # Populate the near cache
        for i in range(ENTRY_COUNT):
            await map.get(i)

        rc.terminateCluster(cluster.id)
        rc.exit()
        await self.assertTrueEventually(lambda: self.assertEqual(1, len(collector.events)))
        self.client = client
        self.map = map

    async def tearDown(self):
        await self.client.shutdown()

    async def test_get_existing_key_from_cache_when_the_cluster_is_down(self):
        for i in range(ENTRY_COUNT):
            self.assertEqual(i, await self.map.get(i))

    async def test_get_non_existing_key_from_cache_when_the_cluster_is_down(self):
        with self.assertRaises(ClientOfflineError):
            await self.map.get(ENTRY_COUNT)

    async def test_put_to_map_when_the_cluster_is_down(self):
        with self.assertRaises(ClientOfflineError):
            await self.map.put(ENTRY_COUNT, ENTRY_COUNT)

    @staticmethod
    def read_cluster_config():
        path = os.path.abspath(__file__)
        dir_path = os.path.dirname(path)
        with open(os.path.join(dir_path, "../../backward_compatible/proxy/hazelcast.xml")) as f:
            return f.read()
