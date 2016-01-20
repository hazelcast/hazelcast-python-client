from hazelcast import ClientConfig
from hazelcast.lifecycle import LIFECYCLE_STATE_SHUTDOWN, LIFECYCLE_STATE_SHUTTING_DOWN, LIFECYCLE_STATE_CONNECTED, \
    LIFECYCLE_STATE_STARTING, LIFECYCLE_STATE_DISCONNECTED
from tests.base import HazelcastTestCase
from tests.util import configure_logging, event_collector


class LifecycleTest(HazelcastTestCase):
    rc = None

    def setUp(self):
        configure_logging()
        self.rc = self.create_rc()
        self.cluster = self.create_cluster(self.rc)

    def tearDown(self):
        self.shutdown_all_clients()
        self.rc.exit()

    def test_lifecycle_listener(self):
        collector = event_collector()
        config = ClientConfig()
        config.lifecycle_listeners = [collector]
        self.cluster.start_member()
        client = self.create_client(config)
        client.shutdown()

        # noinspection PyUnresolvedReferences
        self.assertEqual(collector.events,
                         [LIFECYCLE_STATE_STARTING, LIFECYCLE_STATE_CONNECTED, LIFECYCLE_STATE_SHUTTING_DOWN,
                          LIFECYCLE_STATE_SHUTDOWN])

    def test_lifecycle_listener_disconnected(self):
        collector = event_collector()
        member = self.cluster.start_member()
        client = self.create_client()

        client.lifecycle.add_listener(collector)

        member.shutdown()

        self.assertEqual(collector.events, [LIFECYCLE_STATE_DISCONNECTED])

    def test_remove_lifecycle_listener(self):
        collector = event_collector()

        self.cluster.start_member()
        client = self.create_client()
        id = client.lifecycle.add_listener(collector)
        client.lifecycle.remove_listener(id)
        client.shutdown()

        # noinspection PyUnresolvedReferences
        self.assertEqual(collector.events, [])

    def test_exception_in_listener(self):
        def listener(e):
            raise RuntimeError("error")
        config = ClientConfig()
        config.lifecycle_listeners = [listener]
        self.cluster.start_member()
        self.create_client(config)
