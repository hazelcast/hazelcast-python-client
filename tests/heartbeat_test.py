from hazelcast import HazelcastClient
from hazelcast.core import Address
from tests.base import HazelcastTestCase
from hazelcast.config import ClientConfig, ClientProperties
from tests.util import configure_logging, open_connection_to_address


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
        addr = Address(member2.host, member2.port)
        open_connection_to_address(self.client, addr)
        self.simulate_heartbeat_lost(self.client, addr, 2)

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
