import asyncio
import unittest

from hazelcast.errors import HazelcastClientNotActiveError
from tests.integration.asyncio.base import HazelcastTestCase


class ShutdownTest(unittest.IsolatedAsyncioTestCase, HazelcastTestCase):
    rc = None

    def setUp(self):
        self.rc = self.create_rc()
        self.cluster = self.create_cluster(self.rc)

    async def asyncTearDown(self):
        await self.shutdown_all_clients()
        self.rc.terminateCluster(self.cluster.id)
        self.rc.exit()

    async def test_shutdown_not_hang_on_member_closed(self):
        member = self.cluster.start_member()
        client = await self.create_client(
            {
                "cluster_name": self.cluster.id,
                "cluster_connect_timeout": 5.0,
            }
        )
        my_map = await client.get_map("test")
        await my_map.put("key", "value")
        member.shutdown()
        with self.assertRaises(HazelcastClientNotActiveError):
            while True:
                await my_map.get("key")

    async def test_invocations_finalised_when_client_shutdowns(self):
        self.cluster.start_member()
        client = await self.create_client(
            {
                "cluster_name": self.cluster.id,
            }
        )
        m = await client.get_map("test")
        await m.put("key", "value")

        async def run():
            for _ in range(1000):
                try:
                    await m.get("key")
                except Exception:
                    pass

        async with asyncio.TaskGroup() as tg:
            for _ in range(10):
                tg.create_task(run())

        await client.shutdown()
