import unittest

from hazelcast.asyncio import HazelcastClient
from hazelcast.config import ReconnectMode
from hazelcast.errors import ClientOfflineError, HazelcastClientNotActiveError
from hazelcast.lifecycle import LifecycleState
from tests.integration.asyncio.base import HazelcastTestCase
from tests.util import random_string


class ConnectionStrategyTest(unittest.IsolatedAsyncioTestCase, HazelcastTestCase):
    @classmethod
    def setUpClass(cls):
        cls.rc = cls.create_rc()

    @classmethod
    def tearDownClass(cls):
        cls.rc.exit()

    def setUp(self):
        self.client = None
        self.cluster = None

    async def asyncTearDown(self):
        if self.client:
            await self.client.shutdown()
            self.client = None
        if self.cluster:
            self.rc.terminateCluster(self.cluster.id)
            self.cluster = None

    async def test_off_reconnect_mode(self):
        self.cluster = self.rc.createCluster(None, None)
        member = self.rc.startMember(self.cluster.id)

        def collector():
            events = []

            def on_state_change(event):
                if event == LifecycleState.SHUTDOWN:
                    events.append(event)

            on_state_change.events = events
            return on_state_change

        event_collector = collector()

        self.client = await HazelcastClient.create_and_start(
            cluster_members=["localhost:5701"],
            cluster_name=self.cluster.id,
            reconnect_mode=ReconnectMode.OFF,
            lifecycle_listeners=[event_collector],
        )
        m = await self.client.get_map(random_string())
        # no exception at this point
        await m.put(1, 1)
        self.rc.shutdownMember(self.cluster.id, member.uuid)
        await self.assertTrueEventually(lambda: self.assertEqual(1, len(event_collector.events)))
        with self.assertRaises(HazelcastClientNotActiveError):
            await m.put(1, 1)

    async def test_async_reconnect_mode(self):
        import logging

        logging.basicConfig(level=logging.DEBUG)
        self.cluster = self.rc.createCluster(None, None)
        member = self.rc.startMember(self.cluster.id)

        def collector(event_type):
            events = []

            def on_state_change(event):
                if event == event_type:
                    events.append(event)

            on_state_change.events = events
            return on_state_change

        disconnected_collector = collector(LifecycleState.DISCONNECTED)
        self.client = await HazelcastClient.create_and_start(
            cluster_members=["localhost:5701"],
            cluster_name=self.cluster.id,
            reconnect_mode=ReconnectMode.ASYNC,
            lifecycle_listeners=[disconnected_collector],
        )
        m = await self.client.get_map(random_string())
        # no exception at this point
        await m.put(1, 1)
        self.rc.shutdownMember(self.cluster.id, member.uuid)
        await self.assertTrueEventually(
            lambda: self.assertEqual(1, len(disconnected_collector.events))
        )
        with self.assertRaises(ClientOfflineError):
            await m.put(1, 1)
        connected_collector = collector(LifecycleState.CONNECTED)
        self.client.lifecycle_service.add_listener(connected_collector)
        self.rc.startMember(self.cluster.id)
        await self.assertTrueEventually(
            lambda: self.assertEqual(1, len(connected_collector.events))
        )
        await m.put(1, 1)
