import threading
import time
import random

from tests.base import SingleMemberTestCase, HazelcastTestCase
from tests.hzrc.ttypes import Lang
from tests.util import configure_logging, set_attr
from hazelcast.config import ClientConfig, FlakeIdGeneratorConfig, MAXIMUM_PREFETCH_COUNT
from hazelcast.client import HazelcastClient
from hazelcast.util import to_millis
from hazelcast.proxy.flake_id_generator import _IdBatch, _Block, _AutoBatcher
from hazelcast.future import ImmediateFuture
from hazelcast.exception import HazelcastError

FLAKE_ID_STEP = 1 << 16
SHORT_TERM_BATCH_SIZE = 3
SHORT_TERM_VALIDITY_SECONDS = 3
NUM_THREADS = 4
NUM_IDS_IN_THREADS = 100000
AUTO_BATCHER_BASE = 10


@set_attr(category=3.10)
class FlakeIdGeneratorConfigTest(HazelcastTestCase):
    def setUp(self):
        self.flake_id_config = FlakeIdGeneratorConfig()

    def test_default_configuration(self):
        self.assertEqual("default", self.flake_id_config.name)
        self.assertEqual(100, self.flake_id_config.prefetch_count)
        self.assertEqual(600000, self.flake_id_config.prefetch_validity_in_millis)

    def test_custom_configuration(self):
        self.flake_id_config.name = "test"
        self.flake_id_config.prefetch_count = 333
        self.flake_id_config.prefetch_validity_in_millis = 3333

        self.assertEqual("test", self.flake_id_config.name)
        self.assertEqual(333, self.flake_id_config.prefetch_count)
        self.assertEqual(3333, self.flake_id_config.prefetch_validity_in_millis)

    def test_prefetch_count_should_be_positive(self):
        with self.assertRaises(ValueError):
            self.flake_id_config.prefetch_count = 0

        with self.assertRaises(ValueError):
            self.flake_id_config.prefetch_count = -1

    def test_prefetch_count_max_size(self):
        with self.assertRaises(ValueError):
            self.flake_id_config.prefetch_count = MAXIMUM_PREFETCH_COUNT + 1


@set_attr(category=3.10)
class FlakeIdGeneratorTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        flake_id_config = FlakeIdGeneratorConfig("short-term")
        flake_id_config.prefetch_count = SHORT_TERM_BATCH_SIZE
        flake_id_config.prefetch_validity_in_millis = to_millis(SHORT_TERM_VALIDITY_SECONDS)
        config.add_flake_id_generator_config(flake_id_config)
        return config

    def setUp(self):
        self.flake_id_generator = self.client.get_flake_id_generator("test").blocking()

    def tearDown(self):
        self.flake_id_generator.destroy()

    def test_new_id(self):
        flake_id = self.flake_id_generator.new_id()
        self.assertIsNotNone(flake_id)

    def test_init(self):
        current_id = self.flake_id_generator.new_id()
        self.assertTrue(self.flake_id_generator.init(current_id / 2))
        self.assertFalse(self.flake_id_generator.init(current_id * 2))

    def test_new_id_generates_unique_ids(self):
        id_set = set()

        for i in range(10):
            id_set.add(self.flake_id_generator.new_id())

        self.assertEqual(10, len(id_set))

    def test_new_id_generates_unique_ids_multi_threaded(self):
        id_set = set()

        def func():
            for i in range(NUM_IDS_IN_THREADS):
                # set.add(e) is thread-safe under the CPython interpreter
                id_set.add(self.flake_id_generator.new_id())

        threads = [threading.Thread(target=func) for _ in range(NUM_THREADS)]

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        self.assertEqual(NUM_THREADS * NUM_IDS_IN_THREADS, len(id_set))

    def test_subsequent_ids_are_from_same_batch(self):
        first_id = self.flake_id_generator.new_id()
        second_id = self.flake_id_generator.new_id()
        self.assertEqual(second_id, first_id + FLAKE_ID_STEP)

    def test_ids_are_from_new_batch_after_validity_period(self):
        flake_id_generator = self.client.get_flake_id_generator("short-term").blocking()
        first_id = flake_id_generator.new_id()

        time.sleep(SHORT_TERM_VALIDITY_SECONDS + 1)

        second_id = flake_id_generator.new_id()

        self.assertGreater(second_id, first_id + FLAKE_ID_STEP * SHORT_TERM_BATCH_SIZE)

        flake_id_generator.destroy()

    def test_ids_are_from_new_batch_after_batch_is_exhausted(self):
        flake_id_generator = self.client.get_flake_id_generator("short-term").blocking()

        first_id = flake_id_generator.new_id()
        for i in range(1, SHORT_TERM_BATCH_SIZE):
            flake_id_generator.new_id()

        # Batch is exhausted. We should wait for a little so that the member
        # sends the new batch with a base greater than the last id
        # generated + flake id step size.
        time.sleep(1)

        second_id = flake_id_generator.new_id()
        self.assertGreater(second_id, first_id + FLAKE_ID_STEP * SHORT_TERM_BATCH_SIZE)

        flake_id_generator.destroy()


@set_attr(category=3.10)
class FlakeIdGeneratorDataStructuresTest(HazelcastTestCase):
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
        block = _Block(id_batch, 1)

        time.sleep(0.5)

        self.assertTrueEventually(lambda: block.next_id() is None)

    def test_block_with_batch_exhaustion(self):
        id_batch = _IdBatch(100, 10000, 0)
        block = _Block(id_batch, 1000)

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


@set_attr(category=3.10)
class FlakeIdGeneratorIdOutOfRangeTest(HazelcastTestCase):
    @classmethod
    def setUpClass(cls):
        configure_logging()

    def setUp(self):
        self.rc = self.create_rc()
        self.cluster = self.create_cluster(self.rc, None)
        self.cluster.start_member()
        self.cluster.start_member()

    def tearDown(self):
        self.rc.exit()

    def test_new_id_with_at_least_one_suitable_member(self):
        response = self._assign_out_of_range_node_id(self.cluster.id, random.randint(0, 1))
        self.assertTrueEventually(lambda: response.success and response.result is not None)

        config = ClientConfig()
        config.network_config.smart_routing = False
        client = HazelcastClient(config)

        generator = client.get_flake_id_generator("test").blocking()

        for i in range(100):
            generator.new_id()

        generator.destroy()
        client.shutdown()

    def test_new_id_fails_when_all_members_are_out_of_node_id_range(self):
        response1 = self._assign_out_of_range_node_id(self.cluster.id, 0)
        self.assertTrueEventually(lambda: response1.success and response1.result is not None)

        response2 = self._assign_out_of_range_node_id(self.cluster.id, 1)
        self.assertTrueEventually(lambda: response2.success and response2.result is not None)

        client = HazelcastClient()
        generator = client.get_flake_id_generator("test").blocking()

        with self.assertRaises(HazelcastError):
            generator.new_id()

        generator.destroy()
        client.shutdown()

    def _assign_out_of_range_node_id(self, cluster_id, instance_id):
        script = "def assign_out_of_range_node_id():\n" \
            "\tinstance_{}.getCluster().getLocalMember().setMemberListJoinVersion(100000)\n" \
            "\treturn instance_{}.getCluster().getLocalMember().getMemberListJoinVersion()\n" \
            "result = str(assign_out_of_range_node_id())\n".format(instance_id, instance_id)

        return self.rc.executeOnController(cluster_id, script, Lang.PYTHON)
