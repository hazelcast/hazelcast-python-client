import sys
import time
from threading import Thread, Event

from hazelcast import HazelcastClient
from hazelcast.errors import HazelcastError, TargetDisconnectedError
from hazelcast.lifecycle import LifecycleState
from hazelcast.util import AtomicInteger
from tests.base import HazelcastTestCase
from tests.util import event_collector


class ReconnectTest(HazelcastTestCase):
    rc = None

    def setUp(self):
        self.rc = self.create_rc()
        self.cluster = self.create_cluster(self.rc)

    def tearDown(self):
        self.shutdown_all_clients()
        self.rc.exit()

    def test_start_client_with_no_member(self):
        with self.assertRaises(HazelcastError):
            self.create_client(
                {
                    "cluster_members": [
                        "127.0.0.1:5701",
                        "127.0.0.1:5702",
                        "127.0.0.1:5703",
                    ],
                    "cluster_connect_timeout": 2,
                }
            )

    def test_start_client_before_member(self):
        def run():
            time.sleep(1.0)
            self.cluster.start_member()

        t = Thread(target=run)
        t.start()
        self.create_client(
            {
                "cluster_name": self.cluster.id,
                "cluster_connect_timeout": 5.0,
            }
        )
        t.join()

    def test_restart_member(self):
        member = self.cluster.start_member()
        client = self.create_client(
            {
                "cluster_name": self.cluster.id,
                "cluster_connect_timeout": 5.0,
            }
        )

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
        client = self.create_client(
            {
                "cluster_name": self.cluster.id,
                "cluster_connect_timeout": 5.0,
            }
        )

        map = client.get_map("map").blocking()

        collector = event_collector()
        reg_id = map.add_entry_listener(added_func=collector)
        self.logger.info("Registered listener with id %s", reg_id)
        member.shutdown()
        self.cluster.start_member()

        count = AtomicInteger()

        def assert_events():
            if client.lifecycle_service.is_running():
                while True:
                    try:
                        map.put("key-%d" % count.get_and_increment(), "value")
                        break
                    except TargetDisconnectedError:
                        pass
                self.assertGreater(len(collector.events), 0)
            else:
                self.fail("Client disconnected...")

        self.assertTrueEventually(assert_events)

    def test_member_list_after_reconnect(self):
        old_member = self.cluster.start_member()
        client = self.create_client(
            {
                "cluster_name": self.cluster.id,
                "cluster_connect_timeout": 5.0,
            }
        )
        old_member.shutdown()

        new_member = self.cluster.start_member()

        def assert_member_list():
            members = client.cluster_service.get_members()
            self.assertEqual(1, len(members))
            self.assertEqual(new_member.uuid, str(members[0].uuid))

        self.assertTrueEventually(assert_member_list)

    def test_reconnect_toNewNode_ViaLastMemberList(self):
        old_member = self.cluster.start_member()
        client = self.create_client(
            {
                "cluster_name": self.cluster.id,
                "cluster_members": [
                    "127.0.0.1:5701",
                ],
                "smart_routing": False,
                "cluster_connect_timeout": 10.0,
            }
        )
        new_member = self.cluster.start_member()
        old_member.shutdown()

        def assert_member_list():
            members = client.cluster_service.get_members()
            self.assertEqual(1, len(members))
            self.assertEqual(new_member.uuid, str(members[0].uuid))

        self.assertTrueEventually(assert_member_list)


class ReconnectWithDifferentInterfacesTest(HazelcastTestCase):
    def _create_cluster_config(self, public_address, heartbeat_seconds=300):
        return """<?xml version="1.0" encoding="UTF-8"?>
        <hazelcast xmlns="http://www.hazelcast.com/schema/config"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xsi:schemaLocation="http://www.hazelcast.com/schema/config
            http://www.hazelcast.com/schema/config/hazelcast-config-4.0.xsd">
            <network>
                <public-address>%s</public-address>
            </network>
            <properties>
                <property name="hazelcast.client.max.no.heartbeat.seconds">%d</property>
            </properties>
        </hazelcast>""" % (
            public_address,
            heartbeat_seconds,
        )

    def setUp(self):
        self.rc = self.create_rc()
        self.client = None

    def tearDown(self):
        if self.client:
            # If the test is failed, and we couldn't shutdown
            # the client, try to shutdown here to make sure that
            # we are not going to affect other tests. If the client
            # is already shutdown, then this is basically no-op.
            self.client.shutdown()

        self.rc.exit()

    def test_connection_count_after_reconnect_with_member_hostname_client_ip(self):
        self._verify_connection_count_after_reconnect("localhost", "127.0.0.1")

    def test_connection_count_after_reconnect_with_member_hostname_client_hostname(self):
        self._verify_connection_count_after_reconnect("localhost", "localhost")

    def test_connection_count_after_reconnect_with_member_ip_client_ip(self):
        self._verify_connection_count_after_reconnect("127.0.0.1", "127.0.0.1")

    def test_connection_count_after_reconnect_with_member_ip_client_hostname(self):
        self._verify_connection_count_after_reconnect("127.0.0.1", "localhost")

    def test_listeners_after_client_disconnected_with_member_hostname_client_ip(self):
        self._verify_listeners_after_client_disconnected("localhost", "127.0.0.1")

    def test_listeners_after_client_disconnected_with_member_hostname_client_hostname(self):
        self._verify_listeners_after_client_disconnected("localhost", "localhost")

    def test_listeners_after_client_disconnected_with_member_ip_client_ip(self):
        self._verify_listeners_after_client_disconnected("127.0.0.1", "127.0.0.1")

    def test_listeners_after_client_disconnected_with_member_ip_client_hostname(self):
        self._verify_listeners_after_client_disconnected("127.0.0.1", "localhost")

    def _verify_connection_count_after_reconnect(self, member_address, client_address):
        cluster = self.create_cluster(self.rc, self._create_cluster_config(member_address))
        member = cluster.start_member()

        disconnected = Event()
        reconnected = Event()

        def listener(state):
            if state == "DISCONNECTED":
                disconnected.set()

            if state == "CONNECTED" and disconnected.is_set():
                reconnected.set()

        client = HazelcastClient(
            cluster_name=cluster.id,
            cluster_members=[client_address],
            cluster_connect_timeout=sys.maxsize,
            lifecycle_listeners=[listener],
        )

        self.client = client

        self.assertTrueEventually(
            lambda: self.assertEqual(1, len(client._connection_manager.active_connections))
        )

        member.shutdown()

        self.assertTrueEventually(lambda: self.assertTrue(disconnected.is_set()))

        cluster.start_member()

        self.assertTrueEventually(lambda: self.assertTrue(reconnected.is_set()))

        self.assertEqual(1, len(client._connection_manager.active_connections))

        client.shutdown()
        self.rc.terminateCluster(cluster.id)

    def _verify_listeners_after_client_disconnected(self, member_address, client_address):
        heartbeat_seconds = 2
        cluster = self.create_cluster(
            self.rc, self._create_cluster_config(member_address, heartbeat_seconds)
        )
        member = cluster.start_member()

        client = HazelcastClient(
            cluster_name=cluster.id,
            cluster_members=[client_address],
            cluster_connect_timeout=sys.maxsize,
        )

        self.client = client

        test_map = client.get_map("test").blocking()

        event_count = AtomicInteger()

        test_map.add_entry_listener(
            added_func=lambda _: event_count.get_and_increment(), include_value=False
        )

        self.assertTrueEventually(
            lambda: self.assertEqual(1, len(client._connection_manager.active_connections))
        )

        member.shutdown()

        time.sleep(2 * heartbeat_seconds)

        cluster.start_member()

        def assertion():
            test_map.remove(1)
            test_map.put(1, 2)
            self.assertNotEqual(0, event_count.get())

        self.assertTrueEventually(assertion)

        client.shutdown()
        self.rc.terminateCluster(cluster.id)
