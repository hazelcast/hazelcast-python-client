from threading import Thread
from time import sleep

from hazelcast import ClientConfig
from hazelcast.exception import HazelcastError
from hazelcast.lifecycle import LIFECYCLE_STATE_DISCONNECTED, LIFECYCLE_STATE_CONNECTED
from hazelcast.util import AtomicInteger
from tests.base import HazelcastTestCase
from tests.util import configure_logging, event_collector


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
        config = ClientConfig()
        config.network_config.connection_attempt_limit = 10
        self.create_client(config)

    def test_restart_member(self):
        member = self.cluster.start_member()
        config = ClientConfig()
        config.network_config.connection_attempt_limit = 10
        client = self.create_client(config)

        state = [None]

        def listener(s):
            state[0] = s

        client.lifecycle.add_listener(listener)

        member.shutdown()
        self.assertTrueEventually(lambda: self.assertEqual(state[0], LIFECYCLE_STATE_DISCONNECTED))
        self.cluster.start_member()
        self.assertTrueEventually(lambda: self.assertEqual(state[0], LIFECYCLE_STATE_CONNECTED))

    def test_listener_re_register(self):
        member = self.cluster.start_member()
        config = ClientConfig()
        config.network_config.connection_attempt_limit = 10
        client = self.create_client(config)

        map = client.get_map("map")

        collector = event_collector()
        reg_id = map.add_entry_listener(added_func=collector)
        self.logger.info("Registered listener with id %s", reg_id)
        member.shutdown()
        sleep(3)
        self.cluster.start_member()

        count = AtomicInteger()

        def assert_events():
            if client.lifecycle.is_live:
                map.put("key-%d" % count.get_and_increment(), "value").result()
                self.assertGreater(len(collector.events), 0)
            else:
                self.fail("Client disconnected...")

        self.assertTrueEventually(assert_events)

    def test_member_list_after_reconnect(self):
        old_member = self.cluster.start_member()
        config = ClientConfig()
        config.network_config.connection_attempt_limit = 10
        client = self.create_client(config)
        old_member.shutdown()

        new_member = self.cluster.start_member()

        def assert_member_list():
            self.assertEqual(1, len(client.cluster.members))
            self.assertEqual(new_member.uuid, client.cluster.members[0].uuid)

        self.assertTrueEventually(assert_member_list)
