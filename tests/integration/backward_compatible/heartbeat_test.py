import threading
import time

from hazelcast import HazelcastClient
from hazelcast.core import Address
from tests.base import HazelcastTestCase
from tests.util import open_connection_to_address, wait_for_partition_table


class HeartbeatTest(HazelcastTestCase):
    rc = None

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

        start_simulation = threading.Event()
        assertion_succeeded = [False]
        simulation_thread = self.simulate_heartbeat_loss(
            self.client, addr, 2, start_simulation, assertion_succeeded
        )

        def assert_heartbeat_stopped_and_restored():
            self.assertTrue(len(connection_added_collector.connections) >= 1)
            self.assertTrue(len(connection_removed_collector.connections) >= 1)

            stopped_connection = connection_removed_collector.connections[0]
            restored_connection = connection_added_collector.connections[0]

            self.assertEqual(
                stopped_connection.connected_address,
                Address(member2.host, member2.port),
            )
            self.assertEqual(
                restored_connection.connected_address,
                Address(member2.host, member2.port),
            )
            assertion_succeeded[0] = True

        start_simulation.set()
        self.assertTrueEventually(assert_heartbeat_stopped_and_restored)
        simulation_thread.join()

    @staticmethod
    def simulate_heartbeat_loss(client, address, timeout, start_simulation, assertion_succeeded):
        def run():
            # Wait until the main thread signals we should start the simulation
            start_simulation.wait()

            # It is possible for client to override the set last_read_time
            # of the connection, in case of the periodically sent heartbeat
            # requests getting responses, right after we try to set a new
            # value to it, before the next iteration of the heartbeat manager.
            # In this case, the connection won't be closed, and the test would
            # fail. To avoid it, we will try multiple times.
            for i in range(10):
                if assertion_succeeded[0]:
                    # We have successfully simulated heartbeat loss
                    return

                for connection in client._connection_manager.active_connections.values():
                    if connection.remote_address == address:
                        connection.last_read_time -= timeout
                        break

                time.sleep((i + 1) * 0.1)

        t = threading.Thread(target=run)
        t.start()
        return t
