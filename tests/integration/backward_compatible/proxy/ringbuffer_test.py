import os
import time
import unittest

from hazelcast.proxy.ringbuffer import OVERFLOW_POLICY_FAIL, MAX_BATCH_SIZE
from hazelcast.serialization.api import IdentifiedDataSerializable
from tests.base import SingleMemberTestCase
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
        with open(os.path.join(dir_path, "hazelcast.xml")) as f:
            return f.read()

    def setUp(self):
        self.ringbuffer = self.client.get_ringbuffer(
            "ClientRingbufferTestWithTTL-" + random_string()
        ).blocking()

    def tearDown(self):
        self.ringbuffer.destroy()

    def test_capacity(self):
        self.assertEqual(self.ringbuffer.capacity(), CAPACITY)

    def test_add_size(self):
        self.assertEqual(0, self.ringbuffer.add("value"))
        self.assertEqual(1, self.ringbuffer.add("value"))
        self.assertEqual(2, self.ringbuffer.add("value"))

        self.assertEqual(3, self.ringbuffer.size())

    def test_add_when_full(self):
        self.fill_ringbuffer()

        self.assertEqual(-1, self.ringbuffer.add(CAPACITY + 1, OVERFLOW_POLICY_FAIL))

    def test_add_all(self):
        self.assertEqual(CAPACITY - 1, self.ringbuffer.add_all(list(range(0, CAPACITY))))

    def test_add_all_when_full(self):
        self.assertEqual(
            -1, self.ringbuffer.add_all(list(range(0, CAPACITY * 2)), OVERFLOW_POLICY_FAIL)
        )

    def test_add_all_when_empty_list(self):
        with self.assertRaises(AssertionError):
            self.ringbuffer.add_all([])

    def test_add_all_when_too_large_batch(self):
        with self.assertRaises(AssertionError):
            self.ringbuffer.add_all(list(range(0, MAX_BATCH_SIZE + 1)))

    def test_head_sequence(self):
        self.fill_ringbuffer(CAPACITY * 2)

        self.assertEqual(CAPACITY, self.ringbuffer.head_sequence())

    def test_tail_sequence(self):
        self.fill_ringbuffer(CAPACITY * 2)

        self.assertEqual(CAPACITY * 2 - 1, self.ringbuffer.tail_sequence())

    def test_remaining_capacity(self):
        self.fill_ringbuffer(CAPACITY // 2)

        self.assertEqual(CAPACITY // 2, self.ringbuffer.remaining_capacity())

    def test_read_one(self):
        self.ringbuffer.add("item")
        self.ringbuffer.add("item-2")
        self.ringbuffer.add("item-3")
        self.assertEqual("item", self.ringbuffer.read_one(0))
        self.assertEqual("item-2", self.ringbuffer.read_one(1))
        self.assertEqual("item-3", self.ringbuffer.read_one(2))

    def test_read_one_negative_sequence(self):
        with self.assertRaises(AssertionError):
            self.ringbuffer.read_one(-1)

    def test_read_many(self):
        self.fill_ringbuffer(CAPACITY)
        items = self.ringbuffer.read_many(0, 0, CAPACITY)
        self.assertEqual(items, list(range(0, CAPACITY)))

    def test_read_many_when_negative_start_seq(self):
        with self.assertRaises(AssertionError):
            self.ringbuffer.read_many(-1, 0, CAPACITY)

    def test_read_many_when_min_count_greater_than_max_count(self):
        with self.assertRaises(AssertionError):
            self.ringbuffer.read_many(0, CAPACITY, 0)

    def test_read_many_when_min_count_greater_than_capacity(self):
        with self.assertRaises(AssertionError):
            self.ringbuffer.read_many(0, CAPACITY + 1, CAPACITY + 1)

    def test_read_many_when_max_count_greater_than_batch_size(self):
        with self.assertRaises(AssertionError):
            self.ringbuffer.read_many(0, 0, MAX_BATCH_SIZE + 1)

    def fill_ringbuffer(self, n=CAPACITY):
        for x in range(0, n):
            self.ringbuffer.add(x)

    def test_str(self):
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
        with open(os.path.join(dir_path, "hazelcast.xml")) as f:
            return f.read()

    def setUp(self):
        self.ringbuffer = self.client.get_ringbuffer(
            "ClientRingbufferTestWithTTL-" + random_string()
        ).blocking()

    def tearDown(self):
        self.ringbuffer.destroy()

    def test_when_start_sequence_is_no_longer_available_gets_clamped(self):
        self.fill_ringbuffer(item_count=CAPACITY + 1)

        result_set = self.ringbuffer.read_many(0, 1, CAPACITY)
        self.assertEqual(CAPACITY, result_set.read_count)
        self.assertEqual(CAPACITY, result_set.size)
        self.assertEqual(CAPACITY + 1, result_set.next_sequence_to_read_from)

        for i in range(1, CAPACITY + 1):
            self.assertEqual(i, result_set[i - 1])
            self.assertEqual(i, result_set.get_sequence(i - 1))

    def test_when_start_sequence_is_equal_to_tail_sequence(self):
        self.fill_ringbuffer()

        result_set = self.ringbuffer.read_many(CAPACITY - 1, 1, CAPACITY)
        self.assertEqual(1, result_set.read_count)
        self.assertEqual(1, result_set.size)
        self.assertEqual(CAPACITY, result_set.next_sequence_to_read_from)
        self.assertEqual(CAPACITY - 1, result_set[0])
        self.assertEqual(CAPACITY - 1, result_set.get_sequence(0))

    def test_when_start_sequence_is_beyond_tail_sequence_then_blocks(self):
        self.fill_ringbuffer()

        result_set_future = self.ringbuffer._wrapped.read_many(CAPACITY + 1, 1, CAPACITY)
        time.sleep(0.5)
        self.assertFalse(result_set_future.done())

    def test_when_min_count_items_are_not_available_then_blocks(self):
        self.fill_ringbuffer()

        result_set_future = self.ringbuffer._wrapped.read_many(CAPACITY - 1, 2, 3)
        time.sleep(0.5)
        self.assertFalse(result_set_future.done())

    def test_when_some_waiting_needed(self):
        self.fill_ringbuffer()

        result_set_future = self.ringbuffer._wrapped.read_many(CAPACITY - 1, 2, 3)
        time.sleep(0.5)
        self.assertFalse(result_set_future.done())

        self.ringbuffer.add(CAPACITY)

        self.assertTrueEventually(lambda: self.assertTrue(result_set_future.done()))

        result_set = result_set_future.result()
        self.assertEqual(2, result_set.read_count)
        self.assertEqual(2, result_set.size)
        self.assertEqual(CAPACITY + 1, result_set.next_sequence_to_read_from)
        self.assertEqual(CAPACITY - 1, result_set[0])
        self.assertEqual(CAPACITY - 1, result_set.get_sequence(0))
        self.assertEqual(CAPACITY, result_set[1])
        self.assertEqual(CAPACITY, result_set.get_sequence(1))

    def test_min_zero_when_item_available(self):
        self.fill_ringbuffer()

        result_set = self.ringbuffer.read_many(0, 0, 1)

        self.assertEqual(1, result_set.read_count)
        self.assertEqual(1, result_set.size)

    def test_min_zero_when_no_item_available(self):
        result_set = self.ringbuffer.read_many(0, 0, 1)

        self.assertEqual(0, result_set.read_count)
        self.assertEqual(0, result_set.size)

    def test_max_count(self):
        # If more results are available than needed, the surplus results
        # should not be read.
        self.fill_ringbuffer()

        max_count = CAPACITY // 2
        result_set = self.ringbuffer.read_many(0, 0, max_count)
        self.assertEqual(max_count, result_set.read_count)
        self.assertEqual(max_count, result_set.size)
        self.assertEqual(max_count, result_set.next_sequence_to_read_from)

        for i in range(max_count):
            self.assertEqual(i, result_set[i])
            self.assertEqual(i, result_set.get_sequence(i))

    def test_filter(self):
        def item_factory(i):
            if i % 2 == 0:
                return "good%s" % i
            return "bad%s" % i

        self.fill_ringbuffer(item_factory)

        expected_size = CAPACITY // 2

        result_set = self.ringbuffer.read_many(0, 0, CAPACITY, PrefixFilter("good"))
        self.assertEqual(CAPACITY, result_set.read_count)
        self.assertEqual(expected_size, result_set.size)
        self.assertEqual(CAPACITY, result_set.next_sequence_to_read_from)

        for i in range(expected_size):
            self.assertEqual(item_factory(i * 2), result_set[i])
            self.assertEqual(i * 2, result_set.get_sequence(i))

    def test_filter_with_max_count(self):
        def item_factory(i):
            if i % 2 == 0:
                return "good%s" % i
            return "bad%s" % i

        self.fill_ringbuffer(item_factory)

        expected_size = 3

        result_set = self.ringbuffer.read_many(0, 0, expected_size, PrefixFilter("good"))
        self.assertEqual(expected_size * 2 - 1, result_set.read_count)
        self.assertEqual(expected_size, result_set.size)
        self.assertEqual(expected_size * 2 - 1, result_set.next_sequence_to_read_from)

        for i in range(expected_size):
            self.assertEqual(item_factory(i * 2), result_set[i])
            self.assertEqual(i * 2, result_set.get_sequence(i))

    def fill_ringbuffer(self, item_factory=lambda i: i, item_count=CAPACITY):
        for i in range(0, item_count):
            self.ringbuffer.add(item_factory(i))


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
