import os

from hazelcast.proxy.ringbuffer import OVERFLOW_POLICY_FAIL, MAX_BATCH_SIZE
from tests.base import SingleMemberTestCase
from tests.util import random_string

CAPACITY = 10


class RingBufferTest(SingleMemberTestCase):
    @classmethod
    def configure_cluster(cls):
        return open(os.path.join(os.path.dirname(__file__), "hazelcast_test.xml")).read()

    def setUp(self):
        self.ringbuffer = self.client.get_ringbuffer("ringbuffer-" + random_string()).blocking()

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
        self._fill_ringbuffer()

        self.assertEqual(-1, self.ringbuffer.add(CAPACITY + 1, OVERFLOW_POLICY_FAIL))

    def test_add_all(self):
        self.assertEqual(CAPACITY - 1, self.ringbuffer.add_all(range(0, CAPACITY)))

    def test_add_all_when_full(self):
        self.assertEqual(-1, self.ringbuffer.add_all(range(0, CAPACITY * 2), OVERFLOW_POLICY_FAIL))

    def test_add_all_when_empty_list(self):
        with self.assertRaises(AssertionError):
            self.ringbuffer.add_all([])

    def test_add_all_when_too_large_batch(self):
        with self.assertRaises(AssertionError):
            self.ringbuffer.add_all(range(0, MAX_BATCH_SIZE + 1))

    def test_head_sequence(self):
        self._fill_ringbuffer(CAPACITY * 2)

        self.assertEqual(CAPACITY, self.ringbuffer.head_sequence())

    def test_tail_sequence(self):
        self._fill_ringbuffer(CAPACITY * 2)

        self.assertEqual(CAPACITY * 2 - 1, self.ringbuffer.tail_sequence())

    def test_remaining_capacity(self):
        self._fill_ringbuffer(CAPACITY / 2)

        self.assertEqual(CAPACITY / 2, self.ringbuffer.remaining_capacity())

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
        self._fill_ringbuffer(CAPACITY)
        items = self.ringbuffer.read_many(0, 0, CAPACITY)
        self.assertEqual(items, range(0,CAPACITY))

    def test_read_many_when_negative_start_seq(self):
        with self.assertRaises(AssertionError):
            self.ringbuffer.read_many(-1, 0, CAPACITY)

    def test_read_many_when_min_count_greater_than_max_count(self):
        with self.assertRaises(AssertionError):
            self.ringbuffer.read_many(0, CAPACITY, 0)

    def test_read_many_when_min_count_greater_than_capacity(self):
        with self.assertRaises(AssertionError):
            self.ringbuffer.read_many(0, CAPACITY+1, CAPACITY+1)

    def test_read_many_when_max_count_greater_than_batch_size(self):
        with self.assertRaises(AssertionError):
            self.ringbuffer.read_many(0, 0, MAX_BATCH_SIZE+1)

    def _fill_ringbuffer(self, n=CAPACITY):
        for x in xrange(0, n):
            self.ringbuffer.add(x)

    def test_str(self):
        self.assertTrue(str(self.ringbuffer).startswith("Ringbuffer"))
