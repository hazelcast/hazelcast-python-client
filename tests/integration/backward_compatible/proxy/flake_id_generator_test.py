import threading
import time
import random

from tests.base import SingleMemberTestCase, HazelcastTestCase
from tests.hzrc.ttypes import Lang
from hazelcast.client import HazelcastClient
from hazelcast.errors import HazelcastError

FLAKE_ID_STEP = 1 << 16
SHORT_TERM_BATCH_SIZE = 3
SHORT_TERM_VALIDITY_SECONDS = 3
NUM_THREADS = 4
NUM_IDS_IN_THREADS = 100000


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

    def setUp(self):
        self.flake_id_generator = self.client.get_flake_id_generator("test").blocking()

    def tearDown(self):
        self.flake_id_generator.destroy()

    def test_new_id(self):
        flake_id = self.flake_id_generator.new_id()
        self.assertIsNotNone(flake_id)

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


class FlakeIdGeneratorIdOutOfRangeTest(HazelcastTestCase):
    def setUp(self):
        self.rc = self.create_rc()
        self.cluster = self.create_cluster(self.rc, None)
        self.cluster.start_member()
        self.cluster.start_member()

    def tearDown(self):
        self.rc.terminateCluster(self.cluster.id)
        self.rc.exit()

    def test_new_id_with_at_least_one_suitable_member(self):
        response = self.assign_out_of_range_node_id(self.cluster.id, random.randint(0, 1))
        self.assertTrue(response.success and response.result is not None)

        client = HazelcastClient(cluster_name=self.cluster.id, smart_routing=False)

        generator = client.get_flake_id_generator("test").blocking()

        for i in range(100):
            generator.new_id()

        generator.destroy()
        client.shutdown()

    def test_new_id_fails_when_all_members_are_out_of_node_id_range(self):
        response1 = self.assign_out_of_range_node_id(self.cluster.id, 0)
        self.assertTrue(response1.success and response1.result is not None)

        response2 = self.assign_out_of_range_node_id(self.cluster.id, 1)
        self.assertTrue(response2.success and response2.result is not None)

        client = HazelcastClient(cluster_name=self.cluster.id)
        generator = client.get_flake_id_generator("test").blocking()

        with self.assertRaises(HazelcastError):
            generator.new_id()

        generator.destroy()
        client.shutdown()

    def assign_out_of_range_node_id(self, cluster_id, instance_id):
        script = """
        instance_%s.getCluster().getLocalMember().setMemberListJoinVersion(100000);
        result = "" + instance_%s.getCluster().getLocalMember().getMemberListJoinVersion();
        """ % (
            instance_id,
            instance_id,
        )

        return self.rc.executeOnController(cluster_id, script, Lang.JAVASCRIPT)
