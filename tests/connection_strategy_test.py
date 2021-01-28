from hazelcast import HazelcastClient, six
from hazelcast.config import ReconnectMode
from hazelcast.errors import ClientOfflineError, HazelcastClientNotActiveError
from hazelcast.lifecycle import LifecycleState
from tests.base import HazelcastTestCase
from tests.util import random_string


class ConnectionStrategyTest(HazelcastTestCase):
    @classmethod
    def setUpClass(cls):
        cls.rc = cls.create_rc()

    @classmethod
    def tearDownClass(cls):
        cls.rc.exit()

    def setUp(self):
        self.client = None
        self.cluster = None

    def tearDown(self):
        if self.client:
            self.client.shutdown()
            self.client = None

        if self.cluster:
            self.rc.terminateCluster(self.cluster.id)
            self.cluster = None

    def test_async_start_with_no_cluster(self):
        self.client = HazelcastClient(async_start=True)

        with self.assertRaises(ClientOfflineError):
            self.client.get_map(random_string())

    def test_async_start_with_no_cluster_throws_after_shutdown(self):
        self.client = HazelcastClient(async_start=True)

        self.client.shutdown()
        with self.assertRaises(HazelcastClientNotActiveError):
            self.client.get_map(random_string())

    def test_async_start(self):
        self.cluster = self.rc.createCluster(None, None)
        self.rc.startMember(self.cluster.id)

        def collector():
            events = []

            def on_state_change(event):
                if event == LifecycleState.CONNECTED:
                    events.append(event)

            on_state_change.events = events
            return on_state_change

        event_collector = collector()

        self.client = HazelcastClient(
            cluster_name=self.cluster.id,
            cluster_members=["localhost:5701"],
            async_start=True,
            lifecycle_listeners=[event_collector],
        )

        self.assertTrueEventually(lambda: self.assertEqual(1, len(event_collector.events)))
        self.client.get_map(random_string())

    def test_off_reconnect_mode(self):
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

        self.client = HazelcastClient(
            cluster_members=["localhost:5701"],
            cluster_name=self.cluster.id,
            reconnect_mode=ReconnectMode.OFF,
            lifecycle_listeners=[event_collector],
        )
        m = self.client.get_map(random_string()).blocking()
        # no exception at this point
        m.put(1, 1)
        self.rc.shutdownMember(self.cluster.id, member.uuid)
        self.assertTrueEventually(lambda: self.assertEqual(1, len(event_collector.events)))

        with self.assertRaises(HazelcastClientNotActiveError):
            m.put(1, 1)

    def test_async_reconnect_mode(self):
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

        self.client = HazelcastClient(
            cluster_members=["localhost:5701"],
            cluster_name=self.cluster.id,
            reconnect_mode=ReconnectMode.ASYNC,
            lifecycle_listeners=[disconnected_collector],
        )
        m = self.client.get_map(random_string()).blocking()
        # no exception at this point
        m.put(1, 1)

        self.rc.shutdownMember(self.cluster.id, member.uuid)
        self.assertTrueEventually(lambda: self.assertEqual(1, len(disconnected_collector.events)))
        with self.assertRaises(ClientOfflineError):
            m.put(1, 1)

        self.rc.startMember(self.cluster.id)

        connected_collector = collector(LifecycleState.CONNECTED)
        self.client.lifecycle_service.add_listener(connected_collector)
        self.assertTrueEventually(lambda: self.assertEqual(1, len(connected_collector.events)))

        m.put(1, 1)

    def test_async_start_with_partition_specific_proxies(self):
        self.client = HazelcastClient(async_start=True)

        with self.assertRaises(ClientOfflineError):
            self.client.get_list(random_string())
