import threading
import time

from parameterized import parameterized

from hazelcast import HazelcastClient
from hazelcast.errors import (
    DistributedObjectDestroyedError,
    IllegalStateError,
)
from tests.integration.backward_compatible.proxy.cp import CPTestCase
from tests.util import get_current_timestamp, random_string

SEMAPHORE_TYPES = [
    "sessionless",
    "sessionaware",
]


class SemaphoreTest(CPTestCase):
    def setUp(self):
        self.semaphore = None

    def tearDown(self):
        if self.semaphore:
            self.semaphore.destroy()

    @parameterized.expand(SEMAPHORE_TYPES)
    def test_semaphore_in_another_group(self, semaphore_type):
        semaphore = self.get_semaphore(semaphore_type, 1)
        another_semaphore = self.client.cp_subsystem.get_semaphore(
            semaphore._wrapped._proxy_name + "@another"
        ).blocking()

        self.assertEqual(1, semaphore.available_permits())
        self.assertEqual(0, another_semaphore.available_permits())
        semaphore.acquire()
        self.assertEqual(0, semaphore.available_permits())
        self.assertEqual(0, semaphore.available_permits())

    @parameterized.expand(SEMAPHORE_TYPES)
    def test_use_after_destroy(self, semaphore_type):
        semaphore = self.get_semaphore(semaphore_type)
        semaphore.destroy()
        # the next destroy call should be ignored
        semaphore.destroy()

        with self.assertRaises(DistributedObjectDestroyedError):
            semaphore.init(1)

        semaphore2 = self.client.cp_subsystem.get_semaphore(
            semaphore._wrapped._proxy_name
        ).blocking()

        with self.assertRaises(DistributedObjectDestroyedError):
            semaphore2.init(1)

    def test_session_aware_semaphore_after_client_shutdown(self):
        semaphore = self.get_semaphore("sessionaware", 1)
        another_client = HazelcastClient(cluster_name=self.cluster.id)
        another_semaphore = another_client.cp_subsystem.get_semaphore(
            semaphore._wrapped._proxy_name
        ).blocking()
        another_semaphore.acquire(1)
        self.assertEqual(0, another_semaphore.available_permits())
        self.assertEqual(0, semaphore.available_permits())
        another_client.shutdown()

        def assertion():
            self.assertEqual(1, semaphore.available_permits())

        self.assertTrueEventually(assertion)

    @parameterized.expand(SEMAPHORE_TYPES)
    def test_init(self, semaphore_type):
        semaphore = self.get_semaphore(semaphore_type)
        self.assertEqual(0, semaphore.available_permits())
        self.assertTrue(semaphore.init(10))
        self.assertEqual(10, semaphore.available_permits())

    @parameterized.expand(SEMAPHORE_TYPES)
    def test_init_when_already_initialized(self, semaphore_type):
        semaphore = self.get_semaphore(semaphore_type)
        self.assertTrue(semaphore.init(5))
        self.assertFalse(semaphore.init(7))
        self.assertEqual(5, semaphore.available_permits())

    @parameterized.expand(SEMAPHORE_TYPES)
    def test_acquire(self, semaphore_type):
        semaphore = self.get_semaphore(semaphore_type, 42)
        self.assertIsNone(semaphore.acquire(2))
        self.assertEqual(40, semaphore.available_permits())
        self.assertIsNone(semaphore.acquire())
        self.assertEqual(39, semaphore.available_permits())

    @parameterized.expand(SEMAPHORE_TYPES)
    def test_acquire_when_not_enough_permits(self, semaphore_type):
        semaphore = self.get_semaphore(semaphore_type, 5)
        f = semaphore._wrapped.acquire(10)
        self.assertFalse(f.done())
        time.sleep(2)
        self.assertFalse(f.done())
        semaphore.destroy()

        with self.assertRaises(DistributedObjectDestroyedError):
            f.result()

    @parameterized.expand(SEMAPHORE_TYPES)
    def test_acquire_blocks_until_someone_releases(self, semaphore_type):
        semaphore = self.get_semaphore(semaphore_type, 1)
        event = threading.Event()
        event2 = threading.Event()

        def run():
            semaphore.acquire(1)
            event.set()
            event2.wait()
            time.sleep(1)
            semaphore.release()

        t = threading.Thread(target=run)
        t.start()
        event.wait()
        start = get_current_timestamp()
        f = semaphore._wrapped.acquire()
        event2.set()
        f.result()
        self.assertGreaterEqual(get_current_timestamp() - start, 1)
        t.join()

    @parameterized.expand(SEMAPHORE_TYPES)
    def test_acquire_blocks_until_semaphore_is_destroyed(self, semaphore_type):
        semaphore = self.get_semaphore(semaphore_type, 1)
        event = threading.Event()
        event2 = threading.Event()

        def run():
            semaphore.acquire(1)
            event.set()
            event2.wait()
            time.sleep(1)
            semaphore.destroy()

        t = threading.Thread(target=run)
        t.start()
        event.wait()
        start = get_current_timestamp()
        f = semaphore._wrapped.acquire()
        event2.set()

        with self.assertRaises(DistributedObjectDestroyedError):
            f.result()

        self.assertGreaterEqual(get_current_timestamp() - start, 1)
        t.join()

    @parameterized.expand(SEMAPHORE_TYPES)
    def test_available_permits(self, semaphore_type):
        semaphore = self.get_semaphore(semaphore_type)
        self.assertEqual(0, semaphore.available_permits())
        semaphore.init(5)
        self.assertEqual(5, semaphore.available_permits())
        semaphore.acquire(3)
        self.assertEqual(2, semaphore.available_permits())

    @parameterized.expand(SEMAPHORE_TYPES)
    def test_drain_permits(self, semaphore_type):
        semaphore = self.get_semaphore(semaphore_type, 20)
        semaphore.acquire(5)
        self.assertEqual(15, semaphore.drain_permits())
        self.assertEqual(0, semaphore.available_permits())

    @parameterized.expand(SEMAPHORE_TYPES)
    def test_drain_permits_when_no_permits(self, semaphore_type):
        semaphore = self.get_semaphore(semaphore_type, 0)
        self.assertEqual(0, semaphore.drain_permits())

    @parameterized.expand(SEMAPHORE_TYPES)
    def test_reduce_permits(self, semaphore_type):
        semaphore = self.get_semaphore(semaphore_type, 10)
        self.assertIsNone(semaphore.reduce_permits(5))
        self.assertEqual(5, semaphore.available_permits())
        self.assertIsNone(semaphore.reduce_permits(0))
        self.assertEqual(5, semaphore.available_permits())

    def test_reduce_permits_on_negative_permits_counter_sessionless(self):
        semaphore = self.get_semaphore("sessionless", 10)
        semaphore.reduce_permits(15)
        self.assertEqual(-5, semaphore.available_permits())
        semaphore.release(10)
        self.assertEqual(5, semaphore.available_permits())

    def test_reduce_permits_on_negative_permits_counter_juc_sessionless(self):
        semaphore = self.get_semaphore("sessionless", 0)
        semaphore.reduce_permits(100)
        semaphore.release(10)
        self.assertEqual(-90, semaphore.available_permits())
        self.assertEqual(-90, semaphore.drain_permits())
        semaphore.release(10)
        self.assertEqual(10, semaphore.available_permits())
        self.assertEqual(10, semaphore.drain_permits())

    def test_reduce_permits_on_negative_permits_counter_session_aware(self):
        semaphore = self.get_semaphore("sessionaware", 10)
        semaphore.reduce_permits(15)
        self.assertEqual(-5, semaphore.available_permits())

    def test_reduce_permits_on_negative_permits_counter_juc_session_aware(self):
        semaphore = self.get_semaphore("sessionaware", 0)
        semaphore.reduce_permits(100)
        self.assertEqual(-100, semaphore.available_permits())
        self.assertEqual(-100, semaphore.drain_permits())

    @parameterized.expand(SEMAPHORE_TYPES)
    def test_increase_permits(self, semaphore_type):
        semaphore = self.get_semaphore(semaphore_type, 10)
        self.assertEqual(10, semaphore.available_permits())
        self.assertIsNone(semaphore.increase_permits(100))
        self.assertEqual(110, semaphore.available_permits())
        self.assertIsNone(semaphore.increase_permits(0))
        self.assertEqual(110, semaphore.available_permits())

    @parameterized.expand(SEMAPHORE_TYPES)
    def test_release(self, semaphore_type):
        semaphore = self.get_semaphore(semaphore_type, 2)
        semaphore.acquire(2)
        self.assertIsNone(semaphore.release(2))
        self.assertEqual(2, semaphore.available_permits())

    def test_release_when_acquired_by_another_client_sessionless(self):
        semaphore = self.get_semaphore("sessionless")
        another_client = HazelcastClient(cluster_name=self.cluster.id)
        another_semaphore = another_client.cp_subsystem.get_semaphore(
            semaphore._wrapped._proxy_name
        ).blocking()
        self.assertTrue(another_semaphore.init(1))
        another_semaphore.acquire()

        try:
            semaphore.release(1)
            self.assertEqual(1, semaphore.available_permits())
        finally:
            another_client.shutdown()

    def test_release_when_not_acquired_session_aware(self):
        semaphore = self.get_semaphore("sessionaware", 3)
        semaphore.acquire(1)

        with self.assertRaises(IllegalStateError):
            semaphore.release(2)

    def test_release_when_there_is_no_session_session_aware(self):
        semaphore = self.get_semaphore("sessionaware", 3)

        with self.assertRaises(IllegalStateError):
            semaphore.release()

    @parameterized.expand(SEMAPHORE_TYPES)
    def test_test_try_acquire(self, semaphore_type):
        semaphore = self.get_semaphore(semaphore_type, 5)
        self.assertTrue(semaphore.try_acquire())
        self.assertEqual(4, semaphore.available_permits())

    @parameterized.expand(SEMAPHORE_TYPES)
    def test_try_acquire_with_given_permits(self, semaphore_type):
        semaphore = self.get_semaphore(semaphore_type, 5)
        self.assertTrue(semaphore.try_acquire(3))
        self.assertEqual(2, semaphore.available_permits())

    @parameterized.expand(SEMAPHORE_TYPES)
    def test_try_acquire_when_not_enough_permits(self, semaphore_type):
        semaphore = self.get_semaphore(semaphore_type, 1)
        self.assertFalse(semaphore.try_acquire(2))
        self.assertEqual(1, semaphore.available_permits())

    @parameterized.expand(SEMAPHORE_TYPES)
    def test_try_acquire_when_not_enough_permits_with_timeout(self, semaphore_type):
        semaphore = self.get_semaphore(semaphore_type, 1)
        start = get_current_timestamp()
        self.assertFalse(semaphore.try_acquire(2, 1))
        self.assertGreaterEqual(get_current_timestamp() - start, 1)
        self.assertEqual(1, semaphore.available_permits())

    def get_semaphore(self, semaphore_type, initialize_with=None):
        semaphore = self.client.cp_subsystem.get_semaphore(
            semaphore_type + random_string()
        ).blocking()
        if initialize_with is not None:
            semaphore.init(initialize_with)
        self.semaphore = semaphore
        return semaphore
