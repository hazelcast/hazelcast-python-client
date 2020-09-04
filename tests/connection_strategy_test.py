from hazelcast import ClientConfig, HazelcastClient, six
from hazelcast.config import RECONNECT_MODE
from hazelcast.errors import ClientOfflineError, HazelcastClientNotActiveError
from hazelcast.lifecycle import LifecycleState
from tests.base import HazelcastTestCase
from tests.util import random_string, configure_logging


class ConnectionStrategyTest(HazelcastTestCase):
    @classmethod
    def setUpClass(cls):
        configure_logging()
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
        config = ClientConfig()
        config.connection_strategy.async_start = True
        self.client = HazelcastClient(config)

        with self.assertRaises(ClientOfflineError):
            self.client.get_map(random_string())

    def test_async_start_with_no_cluster_throws_after_shutdown(self):
        config = ClientConfig()
        config.connection_strategy.async_start = True
        self.client = HazelcastClient(config)

        self.client.shutdown()
        with self.assertRaises(HazelcastClientNotActiveError):
            self.client.get_map(random_string())

    def test_async_start(self):
        self.cluster = self.rc.createCluster(None, None)
        self.rc.startMember(self.cluster.id)
        config = ClientConfig()
        config.cluster_name = self.cluster.id
        config.network.addresses.append("localhost:5701")
        config.connection_strategy.async_start = True

        def collector():
            events = []

            def on_state_change(event):
                if event == LifecycleState.CONNECTED:
                    events.append(event)

            on_state_change.events = events
            return on_state_change
        event_collector = collector()
        config.add_lifecycle_listener(event_collector)
        self.client = HazelcastClient(config)

        self.assertTrueEventually(lambda: self.assertEqual(1, len(event_collector.events)))
        self.client.get_map(random_string())

    def test_off_reconnect_mode(self):
        self.cluster = self.rc.createCluster(None, None)
        member = self.rc.startMember(self.cluster.id)
        config = ClientConfig()
        config.cluster_name = self.cluster.id
        config.network.addresses.append("localhost:5701")
        config.connection_strategy.reconnect_mode = RECONNECT_MODE.OFF
        config.connection_strategy.connection_retry.cluster_connect_timeout = six.MAXSIZE

        def collector():
            events = []

            def on_state_change(event):
                if event == LifecycleState.SHUTDOWN:
                    events.append(event)

            on_state_change.events = events
            return on_state_change
        event_collector = collector()
        config.add_lifecycle_listener(event_collector)
        self.client = HazelcastClient(config)
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
        config = ClientConfig()
        config.cluster_name = self.cluster.id
        config.network.addresses.append("localhost:5701")
        config.connection_strategy.reconnect_mode = RECONNECT_MODE.ASYNC
        config.connection_strategy.connection_retry.cluster_connect_timeout = six.MAXSIZE

        def collector(event_type):
            events = []

            def on_state_change(event):
                if event == event_type:
                    events.append(event)

            on_state_change.events = events
            return on_state_change
        disconnected_collector = collector(LifecycleState.DISCONNECTED)
        config.add_lifecycle_listener(disconnected_collector)
        self.client = HazelcastClient(config)
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
        config = ClientConfig()
        config.connection_strategy.async_start = True
        self.client = HazelcastClient(config)

        with self.assertRaises(ClientOfflineError):
            self.client.get_list(random_string())

