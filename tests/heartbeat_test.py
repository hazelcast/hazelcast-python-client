import time
import itertools

from hazelcast import HazelcastClient
from hazelcast.core import Address
from tests.base import HazelcastTestCase
from hazelcast.config import ClientConfig, ClientProperties
from tests.util import configure_logging


class HeartbeatTest(HazelcastTestCase):
    @classmethod
    def setUpClass(cls):
        configure_logging()
        cls.rc = cls.create_rc()

    @classmethod
    def tearDownClass(cls):
        cls.rc.exit()

    def setUp(self):
        self.cluster = self.create_cluster(self.rc)
        self.member = self.rc.startMember(self.cluster.id)
        self.config = ClientConfig()

        self.config.set_property(ClientProperties.HEARTBEAT_INTERVAL.name, 500)
        self.config.set_property(ClientProperties.HEARTBEAT_TIMEOUT.name, 2000)

        self.client = HazelcastClient(self.config)

    def tearDown(self):
        self.client.shutdown()
        self.rc.shutdownCluster(self.cluster.id)

    def test_heartbeat_stopped(self):

        def member_added_func(m):
            retry_count = itertools.count(3, -1)

            def connection_callback(f):
                try:
                    conn = f.result()
                    self.simulate_heartbeat_lost(self.client, Address(conn._address[0], conn._address[1]), 2)
                except:
                    if next(retry_count) > 0:
                        time.sleep(1)
                        self.client.connection_manager.get_or_connect(m.address).add_done_callback(connection_callback)
                    else:
                        self.fail("Couldn't connect to address {}".format(m.address))

            self.client.connection_manager.get_or_connect(m.address).add_done_callback(connection_callback)

        self.client.cluster.add_listener(member_added=member_added_func)

        def connection_collector():
            connections = []

            def collector(c):
                connections.append(c)

            collector.connections = connections
            return collector

        heartbeat_stopped_collector = connection_collector()
        heartbeat_restored_collector = connection_collector()

        self.client.heartbeat.add_listener(on_heartbeat_stopped=heartbeat_stopped_collector,
                                           on_heartbeat_restored=heartbeat_restored_collector)

        member2 = self.rc.startMember(self.cluster.id)

        def assert_heartbeat_stopped_and_restored():
            self.assertEqual(1, len(heartbeat_stopped_collector.connections))
            self.assertEqual(1, len(heartbeat_restored_collector.connections))
            connection_stopped = heartbeat_stopped_collector.connections[0]
            connection_restored = heartbeat_restored_collector.connections[0]
            self.assertEqual(connection_stopped._address, (member2.host, member2.port))
            self.assertEqual(connection_restored._address, (member2.host, member2.port))

        self.assertTrueEventually(assert_heartbeat_stopped_and_restored)

    @staticmethod
    def simulate_heartbeat_lost(client, address, timeout):
        client.connection_manager.connections[address].last_read_in_seconds -= timeout
