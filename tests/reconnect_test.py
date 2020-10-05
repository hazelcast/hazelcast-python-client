from threading import Thread
from time import sleep

from hazelcast import ClientConfig
from hazelcast.errors import HazelcastError, TargetDisconnectedError
from hazelcast.lifecycle import LifecycleState
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
        config.network.addresses.append("127.0.0.1:5701")
        config.network.addresses.append("127.0.0.1:5702")
        config.network.addresses.append("127.0.0.1:5703")
        config.connection_strategy.connection_retry.cluster_connect_timeout = 2
        with self.assertRaises(HazelcastError):
            self.create_client(config)

    def test_start_client_before_member(self):
        t = Thread(target=self.cluster.start_member)
        t.start()
        config = ClientConfig()
        config.cluster_name = self.cluster.id
        config.connection_strategy.connection_retry.cluster_connect_timeout = 5
        self.create_client(config)
        t.join()

    def test_restart_member(self):
        member = self.cluster.start_member()
        config = ClientConfig()
        config.cluster_name = self.cluster.id
        config.connection_strategy.connection_retry.cluster_connect_timeout = 5
        client = self.create_client(config)

        state = [None]

        def listener(s):
            state[0] = s

        client.lifecycle_service.add_listener(listener)

        member.shutdown()
        self.assertTrueEventually(lambda: self.assertEqual(state[0], LifecycleState.DISCONNECTED))
        self.cluster.start_member()
        self.assertTrueEventually(lambda: self.assertEqual(state[0], LifecycleState.CONNECTED))

    def test_listener_re_register(self):
        member = self.cluster.start_member()
        config = ClientConfig()
        config.cluster_name = self.cluster.id
        config.connection_strategy.connection_retry.cluster_connect_timeout = 5
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
            if client.lifecycle_service.is_running():
                while True:
                    try:
                        map.put("key-%d" % count.get_and_increment(), "value").result()
                        break
                    except TargetDisconnectedError:
                        pass
                self.assertGreater(len(collector.events), 0)
            else:
                self.fail("Client disconnected...")

        self.assertTrueEventually(assert_events)

    def test_member_list_after_reconnect(self):
        old_member = self.cluster.start_member()
        config = ClientConfig()
        config.cluster_name = self.cluster.id
        config.connection_strategy.connection_retry.cluster_connect_timeout = 5
        client = self.create_client(config)
        old_member.shutdown()

        new_member = self.cluster.start_member()

        def assert_member_list():
            members = client.cluster_service.get_members()
            self.assertEqual(1, len(members))
            self.assertEqual(new_member.uuid, str(members[0].uuid))

        self.assertTrueEventually(assert_member_list)

    def test_reconnect_toNewNode_ViaLastMemberList(self):
        old_member = self.cluster.start_member()
        config = ClientConfig()
        config.cluster_name = self.cluster.id
        config.network.addresses.append("127.0.0.1:5701")
        config.network.smart_routing = False
        config.connection_strategy.connection_retry.cluster_connect_timeout = 10
        client = self.create_client(config)
        new_member = self.cluster.start_member()
        old_member.shutdown()

        def assert_member_list():
            members = client.cluster_service.get_members()
            self.assertEqual(1, len(members))
            self.assertEqual(new_member.uuid, str(members[0].uuid))

        self.assertTrueEventually(assert_member_list)
