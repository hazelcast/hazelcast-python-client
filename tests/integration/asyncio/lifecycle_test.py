import unittest

from hazelcast.lifecycle import LifecycleState
from tests.integration.asyncio.base import HazelcastTestCase
from tests.util import event_collector


class LifecycleTest(unittest.IsolatedAsyncioTestCase, HazelcastTestCase):
    rc = None

    def setUp(self):
        self.rc = self.create_rc()
        self.cluster = self.create_cluster(self.rc)

    async def asyncTearDown(self):
        await self.shutdown_all_clients()
        self.rc.exit()

    async def test_lifecycle_listener_receives_events_in_order(self):
        collector = event_collector()
        self.cluster.start_member()
        client = await self.create_client(
            {
                "cluster_name": self.cluster.id,
                "lifecycle_listeners": [
                    collector,
                ],
            }
        )
        await client.shutdown()
        self.assertEqual(
            collector.events,
            [
                LifecycleState.STARTING,
                LifecycleState.STARTED,
                LifecycleState.CONNECTED,
                LifecycleState.SHUTTING_DOWN,
                LifecycleState.DISCONNECTED,
                LifecycleState.SHUTDOWN,
            ],
        )

    async def test_lifecycle_listener_receives_events_in_order_after_startup(self):
        self.cluster.start_member()
        collector = event_collector()
        client = await self.create_client(
            {
                "cluster_name": self.cluster.id,
            }
        )
        client.lifecycle_service.add_listener(collector)
        await client.shutdown()
        self.assertEqual(
            collector.events,
            [LifecycleState.SHUTTING_DOWN, LifecycleState.DISCONNECTED, LifecycleState.SHUTDOWN],
        )

    async def test_lifecycle_listener_receives_disconnected_event(self):
        member = self.cluster.start_member()
        collector = event_collector()
        client = await self.create_client(
            {
                "cluster_name": self.cluster.id,
            }
        )
        client.lifecycle_service.add_listener(collector)
        member.shutdown()

        def assertion():
            self.assertEqual(collector.events, [LifecycleState.DISCONNECTED])

        await self.assertTrueEventually(assertion)

        await client.shutdown()

    async def test_remove_lifecycle_listener(self):
        collector = event_collector()
        self.cluster.start_member()
        client = await self.create_client(
            {
                "cluster_name": self.cluster.id,
            }
        )
        registration_id = client.lifecycle_service.add_listener(collector)
        client.lifecycle_service.remove_listener(registration_id)
        await client.shutdown()
        self.assertEqual(collector.events, [])

    async def test_exception_in_listener(self):
        def listener(_):
            raise RuntimeError("error")

        self.cluster.start_member()
        await self.create_client(
            {
                "cluster_name": self.cluster.id,
                "lifecycle_listeners": [
                    listener,
                ],
            }
        )
