import asyncio
import threading
import unittest

from hazelcast.asyncio import HazelcastClient
from hazelcast.core import Address
from tests.integration.asyncio.base import HazelcastTestCase
from tests.integration.asyncio.util import open_connection_to_address, wait_for_partition_table


class HeartbeatTest(unittest.IsolatedAsyncioTestCase, HazelcastTestCase):
    rc = None

    @classmethod
    def setUpClass(cls):
        cls.rc = cls.create_rc()

    @classmethod
    def tearDownClass(cls):
        cls.rc.exit()

    async def asyncSetUp(self):
        self.cluster = self.create_cluster(self.rc)
        self.member = self.rc.startMember(self.cluster.id)
        self.client = await HazelcastClient.create_and_start(
            cluster_name=self.cluster.id,
            heartbeat_interval=0.5,
            heartbeat_timeout=2,
        )

    async def asyncTearDown(self):
        await self.client.shutdown()
        self.rc.shutdownCluster(self.cluster.id)

    async def test_heartbeat_stopped_and_restored(self):
        member2 = self.rc.startMember(self.cluster.id)
        addr = Address(member2.host, member2.port)
        await wait_for_partition_table(self.client)
        await open_connection_to_address(self.client, member2.uuid)

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
        assertion_succeeded = False

        def run():
            nonlocal assertion_succeeded
            # It is possible for client to override the set last_read_time
            # of the connection, in case of the periodically sent heartbeat
            # requests getting responses, right after we try to set a new
            # value to it, before the next iteration of the heartbeat manager.
            # In this case, the connection won't be closed, and the test would
            # fail. To avoid it, we will try multiple times.
            for i in range(10):
                if assertion_succeeded:
                    # We have successfully simulated heartbeat loss
                    return

                for connection in self.client._connection_manager.active_connections.values():
                    if connection.remote_address == addr:
                        connection.last_read_time -= 2
                        break

                asyncio.sleep((i + 1) * 0.1)

        simulation_thread = threading.Thread(target=run)
        simulation_thread.start()

        async def assert_heartbeat_stopped_and_restored():
            nonlocal assertion_succeeded
            self.assertGreaterEqual(len(connection_added_collector.connections), 1)
            self.assertGreaterEqual(len(connection_removed_collector.connections), 1)

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
            assertion_succeeded = True

        await self.assertTrueEventually(assert_heartbeat_stopped_and_restored)
        simulation_thread.join()
