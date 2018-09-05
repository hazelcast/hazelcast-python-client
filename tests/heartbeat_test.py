from hazelcast import HazelcastClient
from hazelcast.core import Address
from tests.base import HazelcastTestCase
from hazelcast.config import ClientConfig, PROPERTY_HEARTBEAT_INTERVAL, PROPERTY_HEARTBEAT_TIMEOUT
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

        self.config._properties[PROPERTY_HEARTBEAT_INTERVAL] = 500
        self.config._properties[PROPERTY_HEARTBEAT_TIMEOUT] = 2000

        self.client = HazelcastClient(self.config)

    def tearDown(self):
        self.client.shutdown()
        self.rc.shutdownCluster(self.cluster.id)

    def test_heartbeat_stopped(self):

        self.client.cluster.add_listener(member_added=lambda m: self.client.connection_manager.get_or_connect(m.address))

        def heartbeat_stopped_collector():
            connections = []

            def connection_collector(c):
                connections.append(c)

            connection_collector.connections = connections
            return connection_collector

        collector = heartbeat_stopped_collector()

        self.client.heartbeat.add_listener(on_heartbeat_stopped=collector)

        member2 = self.rc.startMember(self.cluster.id)
        self.simulate_heartbeat_lost(self.client, Address(member2.host, member2.port), 2)

        def assert_heartbeat_stopped():
            self.assertEqual(1, len(collector.connections))
            connection = collector.connections[0]
            self.assertEqual(connection._address, (member2.host, member2.port))

        self.assertTrueEventually(assert_heartbeat_stopped)

    def test_heartbeat_restored(self):

        def member_added_func(m):

            def connection_callback(f):
                conn = f.result()
                self.simulate_heartbeat_lost(self.client, Address(conn._address[0], conn._address[1]), 2)

            self.client.connection_manager.get_or_connect(m.address).add_done_callback(connection_callback)

        self.client.cluster.add_listener(member_added=member_added_func)

        def heartbeat_restored_collector():
            connections = []

            def connection_collector(c):
                connections.append(c)

            connection_collector.connections = connections
            return connection_collector

        collector = heartbeat_restored_collector()

        self.client.heartbeat.add_listener(on_heartbeat_restored=collector)

        member2 = self.rc.startMember(self.cluster.id)

        def assert_heartbeat_restored():
            self.assertEqual(1, len(collector.connections))
            connection = collector.connections[0]
            self.assertEqual(connection._address, (member2.host, member2.port))

        self.assertTrueEventually(assert_heartbeat_restored)

    @staticmethod
    def simulate_heartbeat_lost(client, address, timeout):
        client.connection_manager.connections[address].last_read -= timeout
