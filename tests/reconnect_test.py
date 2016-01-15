from threading import Thread

from hazelcast import ClientConfig
from hazelcast.exception import HazelcastError
from hazelcast.lifecycle import LIFECYCLE_STATE_DISCONNECTED, LIFECYCLE_STATE_CONNECTED
from tests.base import HazelcastTestCase
from tests.util import configure_logging


class ReconnectTest(HazelcastTestCase):
    rc = None

    def setUp(self):
        configure_logging()
        self.rc = self.create_rc()
        self.cluster = self.create_cluster(self.rc)

    def tearDown(self):
        self.shutdown_all_clients()
        self.rc.exit()

    def test_start_client_with_no_member(self):
        config = ClientConfig()
        config.network_config.addresses.append("127.0.0.1:5701")
        config.network_config.connection_attempt_limit = 2
        config.network_config.connection_attempt_period = 0.1
        with self.assertRaises(HazelcastError):
            self.create_client(config)

    def test_start_client_before_member(self):
        Thread(target=self.cluster.start_member).start()
        self.create_client()

    def test_restart_member(self):
        member = self.cluster.start_member()
        client = self.create_client()

        state = [None]

        def listener(s):
            state[0] = s

        client.lifecycle.add_listener(listener)

        member.shutdown()
        self.assertTrueEventually(lambda: self.assertEqual(state[0], LIFECYCLE_STATE_DISCONNECTED))
        self.cluster.start_member()
        self.assertTrueEventually(lambda: self.assertEqual(state[0], LIFECYCLE_STATE_CONNECTED))


