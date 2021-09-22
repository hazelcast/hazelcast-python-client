from hazelcast import HazelcastClient
from hazelcast.core import Address
from tests.base import HazelcastTestCase
from tests.util import open_connection_to_address, wait_for_partition_table


class HeartbeatTest(HazelcastTestCase):
    @classmethod
    def setUpClass(cls):
        cls.rc = cls.create_rc()

    @classmethod
    def tearDownClass(cls):
        cls.rc.exit()

    def setUp(self):
        self.cluster = self.create_cluster(self.rc)
        self.member = self.rc.startMember(self.cluster.id)
        self.client = HazelcastClient(
            cluster_name=self.cluster.id,
            heartbeat_interval=0.5,
            heartbeat_timeout=2,
        )

    def tearDown(self):
        self.client.shutdown()
        self.rc.shutdownCluster(self.cluster.id)

    def test_heartbeat_stopped_and_restored(self):
        member2 = self.rc.startMember(self.cluster.id)
        addr = Address(member2.host, member2.port)
        wait_for_partition_table(self.client)
        open_connection_to_address(self.client, member2.uuid)

        def connection_collector():
            connections = []

            def collector(c, *_):
                connections.append(c)

            collector.connections = connections
            return collector

        connection_added_collector = connection_collector()
        connection_removed_collector = connection_collector()

        self.client._connection_manager.add_listener(
            connection_added_collector, connection_removed_collector
        )

        self.simulate_heartbeat_lost(self.client, addr, 2)

        def assert_heartbeat_stopped_and_restored():
            self.assertEqual(1, len(connection_added_collector.connections))
            self.assertEqual(1, len(connection_removed_collector.connections))
            stopped_connection = connection_added_collector.connections[0]
            restored_connection = connection_removed_collector.connections[0]
            self.assertEqual(
                stopped_connection.connected_address, Address(member2.host, member2.port)
            )
            self.assertEqual(
                restored_connection.connected_address, Address(member2.host, member2.port)
            )

        self.assertTrueEventually(assert_heartbeat_stopped_and_restored)

    @staticmethod
    def simulate_heartbeat_lost(client, address, timeout):
        for connection in client._connection_manager.active_connections.values():
            if connection.remote_address == address:
                connection.last_read_time -= timeout
                break
