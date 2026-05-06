import asyncio
import random
import unittest

from tests.integration.asyncio.base import SingleMemberTestCase, HazelcastTestCase
from tests.hzrc.ttypes import Lang
from hazelcast.internal.asyncio_client import HazelcastClient
from hazelcast.errors import HazelcastError

FLAKE_ID_STEP = 1 << 16
SHORT_TERM_BATCH_SIZE = 3
SHORT_TERM_VALIDITY_SECONDS = 3
NUM_TASKS = 4
NUM_IDS_IN_TASKS = 100000


class FlakeIdGeneratorTest(SingleMemberTestCase):

    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        config["flake_id_generators"] = {
            "short-term": {
                "prefetch_count": SHORT_TERM_BATCH_SIZE,
                "prefetch_validity": SHORT_TERM_VALIDITY_SECONDS,
            }
        }
        return config

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.flake_id_generator = await self.client.get_flake_id_generator("test")

    async def asyncTearDown(self):
        await self.flake_id_generator.destroy()
        await super().asyncTearDown()

    async def test_new_id(self):
        flake_id = await self.flake_id_generator.new_id()
        self.assertIsNotNone(flake_id)

    async def test_new_id_generates_unique_ids(self):
        id_set = set()
        for i in range(10):
            id_set.add(await self.flake_id_generator.new_id())

        self.assertEqual(10, len(id_set))

    async def test_new_id_generates_unique_ids_concurrent(self):
        id_set = set()

        async def func():
            for i in range(NUM_IDS_IN_TASKS):
                id_set.add(await self.flake_id_generator.new_id())

        async with asyncio.TaskGroup() as tg:
            for _ in range(NUM_TASKS):
                tg.create_task(func())

        self.assertEqual(NUM_TASKS * NUM_IDS_IN_TASKS, len(id_set))

    async def test_subsequent_ids_are_from_same_batch(self):
        first_id = await self.flake_id_generator.new_id()
        second_id = await self.flake_id_generator.new_id()
        self.assertEqual(second_id, first_id + FLAKE_ID_STEP)

    async def test_ids_are_from_new_batch_after_validity_period(self):
        flake_id_generator = await self.client.get_flake_id_generator("short-term")
        first_id = await flake_id_generator.new_id()
        await asyncio.sleep(SHORT_TERM_VALIDITY_SECONDS + 1)
        second_id = await flake_id_generator.new_id()
        self.assertGreater(second_id, first_id + FLAKE_ID_STEP * SHORT_TERM_BATCH_SIZE)
        await flake_id_generator.destroy()

    async def test_ids_are_from_new_batch_after_batch_is_exhausted(self):
        flake_id_generator = await self.client.get_flake_id_generator("short-term")
        first_id = await flake_id_generator.new_id()
        for i in range(1, SHORT_TERM_BATCH_SIZE):
            await flake_id_generator.new_id()

        # Batch is exhausted. We should wait for a little so that the member
        # sends the new batch with a base greater than the last id
        # generated + flake id step size.
        await asyncio.sleep(1)
        second_id = await flake_id_generator.new_id()
        self.assertGreater(second_id, first_id + FLAKE_ID_STEP * SHORT_TERM_BATCH_SIZE)
        await flake_id_generator.destroy()


class FlakeIdGeneratorIdOutOfRangeTest(unittest.IsolatedAsyncioTestCase, HazelcastTestCase):

    def setUp(self):
        self.rc = self.create_rc()
        self.cluster = self.create_cluster(self.rc, None)
        self.cluster.start_member()
        self.cluster.start_member()

    def tearDown(self):
        self.rc.terminateCluster(self.cluster.id)
        self.rc.exit()

    async def test_new_id_with_at_least_one_suitable_member(self):
        response = await self.assign_out_of_range_node_id(self.cluster.id, random.randint(0, 1))
        self.assertTrue(response.success and response.result is not None)
        client = await HazelcastClient.create_and_start(
            cluster_name=self.cluster.id, smart_routing=False
        )
        generator = await client.get_flake_id_generator("test")

        for i in range(100):
            await generator.new_id()

        await generator.destroy()
        await client.shutdown()

    async def test_new_id_fails_when_all_members_are_out_of_node_id_range(self):
        response1 = await self.assign_out_of_range_node_id(self.cluster.id, 0)
        self.assertTrue(response1.success and response1.result is not None)
        response2 = await self.assign_out_of_range_node_id(self.cluster.id, 1)
        self.assertTrue(response2.success and response2.result is not None)
        client = await HazelcastClient.create_and_start(cluster_name=self.cluster.id)
        generator = await client.get_flake_id_generator("test")
        with self.assertRaises(HazelcastError):
            await generator.new_id()

        await generator.destroy()
        await client.shutdown()

    async def assign_out_of_range_node_id(self, cluster_id, instance_id):
        script = """
        instance_%s.getCluster().getLocalMember().setMemberListJoinVersion(100000);
        result = "" + instance_%s.getCluster().getLocalMember().getMemberListJoinVersion();
        """ % (
            instance_id,
            instance_id,
        )
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.rc.executeOnController, cluster_id, script, Lang.JAVASCRIPT)
