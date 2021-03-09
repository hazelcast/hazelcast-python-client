import threading
import time
import unittest

from hazelcast.future import ImmediateFuture
from hazelcast.proxy.flake_id_generator import _IdBatch, _Block, _AutoBatcher

FLAKE_ID_STEP = 1 << 16
AUTO_BATCHER_BASE = 10
NUM_THREADS = 4
NUM_IDS_IN_THREADS = 100000


class FlakeIdGeneratorDataStructuresTest(unittest.TestCase):
    def test_id_batch_as_iterator(self):
        base = 3
        increment = 4
        batch_size = 100

        # number of elements * (first element + last element) / 2
        expected_value = batch_size * (base + (base + (batch_size - 1) * increment)) / 2

        self.assertEqual(expected_value, sum(_IdBatch(base, increment, batch_size)))

    def test_id_batch_stop_iteration_raised(self):
        batch_size = 10
        count = 0

        for _ in _IdBatch(0, 1, batch_size):
            count += 1

        # For loop ends when the iterator raises StopIteration
        self.assertEqual(batch_size, count)

    def test_id_batch_exhaustion_with_non_positive_batch_size(self):
        id_batch = _IdBatch(1, 2, 0)
        iterator = iter(id_batch)

        id_batch2 = _IdBatch(3, 4, -1)
        iterator2 = iter(id_batch2)

        with self.assertRaises(StopIteration):
            next(iterator)

        with self.assertRaises(StopIteration):
            next(iterator2)

    def test_block(self):
        id_batch = _IdBatch(1, 2, 3)
        block = _Block(id_batch, 0)

        self.assertIsNotNone(block.next_id())

    def test_block_after_validity_period(self):
        id_batch = _IdBatch(-1, -2, 2)
        block = _Block(id_batch, 0.1)

        time.sleep(0.5)

        self.assertIsNone(block.next_id())

    def test_block_with_batch_exhaustion(self):
        id_batch = _IdBatch(100, 10000, 0)
        block = _Block(id_batch, 1)

        self.assertIsNone(block.next_id())

    def test_auto_batcher(self):
        auto_batcher = _AutoBatcher(10, 0, self._id_batch_supplier)

        self.assertEqual(AUTO_BATCHER_BASE, auto_batcher.new_id().result())
        self.assertEqual(AUTO_BATCHER_BASE + FLAKE_ID_STEP, auto_batcher.new_id().result())

    def test_auto_batcher_multi_threaded(self):
        id_set = set()
        id_list = list()

        batch_size = 100

        auto_batcher = _AutoBatcher(batch_size, 0, self._id_batch_supplier)

        def func():
            for i in range(NUM_IDS_IN_THREADS):
                fake_id = auto_batcher.new_id().result()
                id_set.add(fake_id)
                id_list.append(fake_id)

        threads = [threading.Thread(target=func) for _ in range(NUM_THREADS)]

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        # We are faking with this batch size and supplier supplies the same base and increment each
        # time. So, we should see unique ids as many as the batch size
        self.assertEqual(batch_size, len(id_set))

        # We should generate this many ids whether or not they are unique
        self.assertEqual(NUM_THREADS * NUM_IDS_IN_THREADS, len(id_list))

    def _id_batch_supplier(self, batch_size):
        return ImmediateFuture(_IdBatch(AUTO_BATCHER_BASE, FLAKE_ID_STEP, batch_size))
