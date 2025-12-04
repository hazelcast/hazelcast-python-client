import asyncio
import unittest

from tests.integration.asyncio.base import HazelcastTestCase
from tests.util import random_string, event_collector


class SmartListenerTest(unittest.IsolatedAsyncioTestCase, HazelcastTestCase):

    rc = None
    cluster = None

    @classmethod
    def setUpClass(cls):
        cls.rc = cls.create_rc()
        cls.cluster = cls.create_cluster(cls.rc, None)
        cls.cluster.start_member()
        cls.cluster.start_member()

    @classmethod
    def tearDownClass(cls):
        cls.rc.terminateCluster(cls.cluster.id)
        cls.rc.exit()

    async def asyncSetUp(self):
        self.client = await self.create_client(
            {
                "cluster_name": self.cluster.id,
                "smart_routing": True,
            }
        )
        self.collector = event_collector()

    async def asyncTearDown(self):
        await self.shutdown_all_clients()

    async def test_map_smart_listener_local_only(self):
        map = await self.client.get_map(random_string())
        await map.add_entry_listener(added_func=self.collector)
        await map.put("key", "value")
        await self.assert_event_received_once()

    async def assert_event_received_once(self):
        await asyncio.sleep(2)
        self.assertEqual(1, len(self.collector.events))
