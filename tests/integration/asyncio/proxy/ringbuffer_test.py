import asyncio
import os
import unittest

from hazelcast.proxy.ringbuffer import OVERFLOW_POLICY_FAIL, MAX_BATCH_SIZE
from hazelcast.serialization.api import IdentifiedDataSerializable
from tests.integration.asyncio.base import SingleMemberTestCase
from tests.util import random_string, compare_client_version

CAPACITY = 10


class RingBufferTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        return config

    @classmethod
    def configure_cluster(cls):
        path = os.path.abspath(__file__)
        dir_path = os.path.dirname(path)
        xml_path = os.path.join(
            dir_path, "../../backward_compatible/proxy/hazelcast.xml"
        )
        with open(xml_path) as f:
            return f.read()

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.ringbuffer = await self.client.get_ringbuffer(
            "ClientRingbufferTestWithTTL-" + random_string()
        )

    async def asyncTearDown(self):
        await self.ringbuffer.destroy()
        await super().asyncTearDown()

    async def test_capacity(self):
        self.assertEqual(await self.ringbuffer.capacity(), CAPACITY)

    async def test_add_size(self):
        self.assertEqual(0, await self.ringbuffer.add("value"))
        self.assertEqual(1, await self.ringbuffer.add("value"))
        self.assertEqual(2, await self.ringbuffer.add("value"))

        self.assertEqual(3, await self.ringbuffer.size())

    async def test_add_when_full(self):
        await self.fill_ringbuffer()

        self.assertEqual(-1, await self.ringbuffer.add(CAPACITY + 1, OVERFLOW_POLICY_FAIL))

    async def test_add_all(self):
        self.assertEqual(CAPACITY - 1, await self.ringbuffer.add_all(list(range(0, CAPACITY))))

    async def test_add_all_when_full(self):
        self.assertEqual(
            -1, await self.ringbuffer.add_all(list(range(0, CAPACITY * 2)), OVERFLOW_POLICY_FAIL)
        )

    async def test_add_all_when_empty_list(self):
        with self.assertRaises(AssertionError):
            await self.ringbuffer.add_all([])

    async def test_add_all_when_too_large_batch(self):
        with self.assertRaises(AssertionError):
            await self.ringbuffer.add_all(list(range(0, MAX_BATCH_SIZE + 1)))

    async def test_head_sequence(self):
        await self.fill_ringbuffer(CAPACITY * 2)

        self.assertEqual(CAPACITY, await self.ringbuffer.head_sequence())

    async def test_tail_sequence(self):
        await self.fill_ringbuffer(CAPACITY * 2)

        self.assertEqual(CAPACITY * 2 - 1, await self.ringbuffer.tail_sequence())

    async def test_remaining_capacity(self):
        await self.fill_ringbuffer(CAPACITY // 2)

        self.assertEqual(CAPACITY // 2, await self.ringbuffer.remaining_capacity())

    async def test_read_one(self):
        await self.ringbuffer.add("item")
        await self.ringbuffer.add("item-2")
        await self.ringbuffer.add("item-3")
        self.assertEqual("item", await self.ringbuffer.read_one(0))
        self.assertEqual("item-2", await self.ringbuffer.read_one(1))
        self.assertEqual("item-3", await self.ringbuffer.read_one(2))

    async def test_read_one_negative_sequence(self):
        with self.assertRaises(AssertionError):
            await self.ringbuffer.read_one(-1)

    async def test_read_many(self):
        await self.fill_ringbuffer(CAPACITY)
        items = await self.ringbuffer.read_many(0, 0, CAPACITY)
        self.assertEqual(items, list(range(0, CAPACITY)))

    async def test_read_many_when_negative_start_seq(self):
        with self.assertRaises(AssertionError):
            await self.ringbuffer.read_many(-1, 0, CAPACITY)

    async def test_read_many_when_min_count_greater_than_max_count(self):
        with self.assertRaises(AssertionError):
            await self.ringbuffer.read_many(0, CAPACITY, 0)

    async def test_read_many_when_min_count_greater_than_capacity(self):
        with self.assertRaises(AssertionError):
            await self.ringbuffer.read_many(0, CAPACITY + 1, CAPACITY + 1)

    async def test_read_many_when_max_count_greater_than_batch_size(self):
        with self.assertRaises(AssertionError):
            await self.ringbuffer.read_many(0, 0, MAX_BATCH_SIZE + 1)

    async def fill_ringbuffer(self, n=CAPACITY):
        for x in range(0, n):
            await self.ringbuffer.add(x)

    async def test_str(self):
        self.assertTrue(str(self.ringbuffer).startswith("Ringbuffer"))


@unittest.skipIf(
    compare_client_version("4.1") < 0, "Tests the features added in 4.1 version of the client"
)
class RingbufferReadManyTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        return config

    @classmethod
    def configure_cluster(cls):
        path = os.path.abspath(__file__)
        dir_path = os.path.dirname(path)
        xml_path = os.path.join(
            dir_path, "../../backward_compatible/proxy/hazelcast.xml"
        )
        with open(xml_path) as f:
            return f.read()

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.ringbuffer = await self.client.get_ringbuffer(
            "ClientRingbufferTestWithTTL-" + random_string()
        )

    async def asyncTearDown(self):
        await self.ringbuffer.destroy()
        await super().asyncTearDown()

    async def test_when_start_sequence_is_no_longer_available_gets_clamped(self):
        await self.fill_ringbuffer(item_count=CAPACITY + 1)

        result_set = await self.ringbuffer.read_many(0, 1, CAPACITY)
        self.assertEqual(CAPACITY, result_set.read_count)
        self.assertEqual(CAPACITY, result_set.size)
        self.assertEqual(CAPACITY + 1, result_set.next_sequence_to_read_from)

        for i in range(1, CAPACITY + 1):
            self.assertEqual(i, result_set[i - 1])
            self.assertEqual(i, result_set.get_sequence(i - 1))

    async def test_when_start_sequence_is_equal_to_tail_sequence(self):
        await self.fill_ringbuffer()

        result_set = await self.ringbuffer.read_many(CAPACITY - 1, 1, CAPACITY)
        self.assertEqual(1, result_set.read_count)
        self.assertEqual(1, result_set.size)
        self.assertEqual(CAPACITY, result_set.next_sequence_to_read_from)
        self.assertEqual(CAPACITY - 1, result_set[0])
        self.assertEqual(CAPACITY - 1, result_set.get_sequence(0))

    async def test_when_start_sequence_is_beyond_tail_sequence_then_blocks(self):
        await self.fill_ringbuffer()

        task = asyncio.create_task(self.ringbuffer.read_many(CAPACITY + 1, 1, CAPACITY))
        await asyncio.sleep(0.5)
        self.assertFalse(task.done())
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    async def test_when_min_count_items_are_not_available_then_blocks(self):
        await self.fill_ringbuffer()

        task = asyncio.create_task(self.ringbuffer.read_many(CAPACITY - 1, 2, 3))
        await asyncio.sleep(0.5)
        self.assertFalse(task.done())
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    async def test_when_some_waiting_needed(self):
        await self.fill_ringbuffer()

        task = asyncio.create_task(self.ringbuffer.read_many(CAPACITY - 1, 2, 3))
        await asyncio.sleep(0.5)
        self.assertFalse(task.done())

        await self.ringbuffer.add(CAPACITY)

        await self.assertTrueEventually(lambda: self.assertTrue(task.done()))

        result_set = task.result()
        self.assertEqual(2, result_set.read_count)
        self.assertEqual(2, result_set.size)
        self.assertEqual(CAPACITY + 1, result_set.next_sequence_to_read_from)
        self.assertEqual(CAPACITY - 1, result_set[0])
        self.assertEqual(CAPACITY - 1, result_set.get_sequence(0))
        self.assertEqual(CAPACITY, result_set[1])
        self.assertEqual(CAPACITY, result_set.get_sequence(1))

    async def test_min_zero_when_item_available(self):
        await self.fill_ringbuffer()

        result_set = await self.ringbuffer.read_many(0, 0, 1)

        self.assertEqual(1, result_set.read_count)
        self.assertEqual(1, result_set.size)

    async def test_min_zero_when_no_item_available(self):
        result_set = await self.ringbuffer.read_many(0, 0, 1)

        self.assertEqual(0, result_set.read_count)
        self.assertEqual(0, result_set.size)

    async def test_max_count(self):
        # If more results are available than needed, the surplus results
        # should not be read.
        await self.fill_ringbuffer()

        max_count = CAPACITY // 2
        result_set = await self.ringbuffer.read_many(0, 0, max_count)
        self.assertEqual(max_count, result_set.read_count)
        self.assertEqual(max_count, result_set.size)
        self.assertEqual(max_count, result_set.next_sequence_to_read_from)

        for i in range(max_count):
            self.assertEqual(i, result_set[i])
            self.assertEqual(i, result_set.get_sequence(i))

    async def test_filter(self):
        def item_factory(i):
            if i % 2 == 0:
                return "good%s" % i
            return "bad%s" % i

        await self.fill_ringbuffer(item_factory)

        expected_size = CAPACITY // 2

        result_set = await self.ringbuffer.read_many(0, 0, CAPACITY, PrefixFilter("good"))
        self.assertEqual(CAPACITY, result_set.read_count)
        self.assertEqual(expected_size, result_set.size)
        self.assertEqual(CAPACITY, result_set.next_sequence_to_read_from)

        for i in range(expected_size):
            self.assertEqual(item_factory(i * 2), result_set[i])
            self.assertEqual(i * 2, result_set.get_sequence(i))

    async def test_filter_with_max_count(self):
        def item_factory(i):
            if i % 2 == 0:
                return "good%s" % i
            return "bad%s" % i

        await self.fill_ringbuffer(item_factory)

        expected_size = 3

        result_set = await self.ringbuffer.read_many(0, 0, expected_size, PrefixFilter("good"))
        self.assertEqual(expected_size * 2 - 1, result_set.read_count)
        self.assertEqual(expected_size, result_set.size)
        self.assertEqual(expected_size * 2 - 1, result_set.next_sequence_to_read_from)

        for i in range(expected_size):
            self.assertEqual(item_factory(i * 2), result_set[i])
            self.assertEqual(i * 2, result_set.get_sequence(i))

    async def fill_ringbuffer(self, item_factory=lambda i: i, item_count=CAPACITY):
        for i in range(0, item_count):
            await self.ringbuffer.add(item_factory(i))


class PrefixFilter(IdentifiedDataSerializable):
    def __init__(self, prefix):
        self.prefix = prefix

    def write_data(self, object_data_output):
        object_data_output.write_string(self.prefix)

    def read_data(self, object_data_input):
        self.prefix = object_data_input.read_string()

    def get_factory_id(self):
        return 666

    def get_class_id(self):
        return 14
