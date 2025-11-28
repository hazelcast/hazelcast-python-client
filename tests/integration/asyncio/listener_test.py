import asyncio
import unittest

from parameterized import parameterized

from tests.integration.asyncio.base import HazelcastTestCase
from tests.integration.asyncio.util import (
    generate_key_owned_by_instance,
    wait_for_partition_table,
)
from tests.util import (
    random_string,
    event_collector,
)

LISTENER_TYPES = [
    (
        "smart",
        True,
    ),
    (
        "non-smart",
        False,
    ),
]


class ListenerRemoveMemberTest(unittest.IsolatedAsyncioTestCase, HazelcastTestCase):
    def setUp(self):
        self.rc = self.create_rc()
        self.cluster = self.create_cluster(self.rc, None)
        self.m1 = self.cluster.start_member()
        self.m2 = self.cluster.start_member()
        self.client_config = {
            "cluster_name": self.cluster.id,
            "heartbeat_interval": 1.0,
        }
        self.collector = event_collector()

    async def asyncTearDown(self):
        await self.shutdown_all_clients()
        self.rc.terminateCluster(self.cluster.id)
        self.rc.exit()

    async def test_remove_member_smart(self):
        await self._remove_member_test(True)

    async def test_remove_member_unisocket(self):
        await self._remove_member_test(False)

    async def _remove_member_test(self, is_smart):
        self.client_config["smart_routing"] = is_smart
        client = await self.create_client(self.client_config)
        await wait_for_partition_table(client)
        key_m1 = generate_key_owned_by_instance(client, self.m1.uuid)
        random_map = await client.get_map(random_string())
        await random_map.add_entry_listener(added_func=self.collector)
        await asyncio.to_thread(self.m1.shutdown)
        await random_map.put(key_m1, "value2")

        def assert_event():
            self.assertEqual(1, len(self.collector.events))

        await self.assertTrueEventually(assert_event)


class ListenerAddMemberTest(unittest.IsolatedAsyncioTestCase, HazelcastTestCase):
    def setUp(self):
        self.rc = self.create_rc()
        self.cluster = self.create_cluster(self.rc, None)
        self.m1 = self.cluster.start_member()
        self.client_config = {
            "cluster_name": self.cluster.id,
        }
        self.collector = event_collector()

    async def asyncTearDown(self):
        await self.shutdown_all_clients()
        self.rc.terminateCluster(self.cluster.id)
        self.rc.exit()

    async def test_add_member_smart(self):
        await self._add_member_test(True)

    async def test_add_member_unisocket(self):
        await self._add_member_test(True)

    async def _add_member_test(self, is_smart):
        self.client_config["smart_routing"] = is_smart
        client = await self.create_client(self.client_config)
        random_map = await client.get_map(random_string())
        await random_map.add_entry_listener(added_func=self.collector, updated_func=self.collector)
        m2 = await asyncio.to_thread(self.cluster.start_member)
        await wait_for_partition_table(client)
        key_m2 = generate_key_owned_by_instance(client, m2.uuid)
        assertion_succeeded = False

        async def run():
            nonlocal assertion_succeeded
            # When a new connection is added, we add the existing
            # listeners to it, but we do it non-blocking. So, it might
            # be the case that, the listener registration request is
            # sent to the new member, but not completed yet.
            # So, we might not get an event for the put. To avoid this,
            # we will put multiple times.
            for i in range(10):
                if assertion_succeeded:
                    # We have successfully got an event
                    return

                await random_map.put(key_m2, f"value-{i}")
                await asyncio.sleep((i + 1) * 0.1)

        asyncio.create_task(run())

        def assert_event():
            nonlocal assertion_succeeded
            self.assertGreaterEqual(len(self.collector.events), 1)
            assertion_succeeded = True

        await self.assertTrueEventually(assert_event)
