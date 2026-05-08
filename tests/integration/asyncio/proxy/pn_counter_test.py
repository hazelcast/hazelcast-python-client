import os
import unittest

from hazelcast.errors import ConsistencyLostError, NoDataMemberInClusterError
from hazelcast.internal.asyncio_client import HazelcastClient
from tests.integration.asyncio.base import SingleMemberTestCase, HazelcastTestCase


class PNCounterBasicTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        return config

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.pn_counter = await self.client.get_pn_counter("pn-counter")

    async def asyncTearDown(self):
        await self.pn_counter.destroy()
        await super().asyncTearDown()

    async def test_get(self):
        await self.pn_counter.add_and_get(4)
        self.assertEqual(4, await self.pn_counter.get())

    async def test_get_initial_value(self):
        self.assertEqual(0, await self.pn_counter.get())

    async def test_get_and_add(self):
        await self.check_pn_counter_method(await self.pn_counter.get_and_add(3), 0, 3)

    async def test_add_and_get(self):
        await self.check_pn_counter_method(await self.pn_counter.add_and_get(4), 4, 4)

    async def test_get_and_subtract(self):
        await self.check_pn_counter_method(await self.pn_counter.get_and_subtract(2), 0, -2)

    async def test_subtract_and_get(self):
        await self.check_pn_counter_method(await self.pn_counter.subtract_and_get(5), -5, -5)

    async def test_get_and_decrement(self):
        await self.check_pn_counter_method(await self.pn_counter.get_and_decrement(), 0, -1)

    async def test_decrement_and_get(self):
        await self.check_pn_counter_method(await self.pn_counter.decrement_and_get(), -1, -1)

    async def test_get_and_increment(self):
        await self.check_pn_counter_method(await self.pn_counter.get_and_increment(), 0, 1)

    async def test_increment_and_get(self):
        await self.check_pn_counter_method(await self.pn_counter.increment_and_get(), 1, 1)

    async def test_reset(self):
        await self.pn_counter.get_and_add(1)
        old_vector_clock = self.pn_counter._observed_clock
        self.pn_counter.reset()
        self.assertNotEqual(old_vector_clock, self.pn_counter._observed_clock)

    async def check_pn_counter_method(
        self, return_value, expected_return_value, expected_get_value
    ):
        get_value = await self.pn_counter.get()

        self.assertEqual(expected_return_value, return_value)
        self.assertEqual(expected_get_value, get_value)


class PNCounterConsistencyTest(unittest.IsolatedAsyncioTestCase, HazelcastTestCase):
    async def asyncSetUp(self):
        self.rc = self.create_rc()
        self.cluster = self.create_cluster(self.rc, self.read_cluster_config())
        self.cluster.start_member()
        self.cluster.start_member()
        self.client = await HazelcastClient.create_and_start(cluster_name=self.cluster.id)
        self.pn_counter = await self.client.get_pn_counter("pn-counter")

    async def asyncTearDown(self):
        await self.client.shutdown()
        self.rc.terminateCluster(self.cluster.id)
        self.rc.exit()

    async def test_consistency_lost_error_raised_when_target_terminates(self):
        await self.pn_counter.add_and_get(3)
        replica_address = self.pn_counter._current_target_replica_address
        self.rc.terminateMember(self.cluster.id, str(replica_address.uuid))
        with self.assertRaises(ConsistencyLostError):
            await self.pn_counter.add_and_get(5)

    async def test_counter_can_continue_session_by_calling_reset(self):
        await self.pn_counter.add_and_get(3)
        replica_address = self.pn_counter._current_target_replica_address
        self.rc.terminateMember(self.cluster.id, str(replica_address.uuid))
        self.pn_counter.reset()
        await self.pn_counter.add_and_get(5)

    @staticmethod
    def read_cluster_config():
        path = os.path.abspath(__file__)
        dir_path = os.path.dirname(path)
        with open(os.path.join(dir_path, "../../backward_compatible/proxy/hazelcast.xml")) as f:
            return f.read()


class PNCounterLiteMemberTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        return config

    @classmethod
    def configure_cluster(cls):
        path = os.path.abspath(__file__)
        dir_path = os.path.dirname(path)
        with open(
            os.path.join(dir_path, "../../backward_compatible/proxy/hazelcast_litemember.xml")
        ) as f:
            return f.read()

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.pn_counter = await self.client.get_pn_counter("pn-counter")

    async def asyncTearDown(self):
        await self.pn_counter.destroy()
        await super().asyncTearDown()

    async def test_get_with_lite_member(self):
        await self.verify_error_raised(NoDataMemberInClusterError, self.pn_counter.get)

    async def test_get_and_add_with_lite_member(self):
        await self.verify_error_raised(NoDataMemberInClusterError, self.pn_counter.get_and_add, 1)

    async def test_add_and_get_with_lite_member(self):
        await self.verify_error_raised(NoDataMemberInClusterError, self.pn_counter.add_and_get, 2)

    async def test_get_and_subtract_with_lite_member(self):
        await self.verify_error_raised(
            NoDataMemberInClusterError, self.pn_counter.get_and_subtract, 1
        )

    async def test_subtract_and_get_with_lite_member(self):
        await self.verify_error_raised(
            NoDataMemberInClusterError, self.pn_counter.subtract_and_get, 5
        )

    async def test_get_and_decrement_with_lite_member(self):
        await self.verify_error_raised(
            NoDataMemberInClusterError, self.pn_counter.get_and_decrement
        )

    async def test_decrement_and_get_with_lite_member(self):
        await self.verify_error_raised(
            NoDataMemberInClusterError, self.pn_counter.decrement_and_get
        )

    async def test_get_and_increment(self):
        await self.verify_error_raised(
            NoDataMemberInClusterError, self.pn_counter.get_and_increment
        )

    async def test_increment_and_get(self):
        await self.verify_error_raised(
            NoDataMemberInClusterError, self.pn_counter.increment_and_get
        )

    async def verify_error_raised(self, error, func, *args):
        with self.assertRaises(error):
            await func(*args)
