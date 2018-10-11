import time
from threading import Event
from tests.base import SingleMemberTestCase
from tests.util import random_string, generate_key_owned_by_instance


class LockTest(SingleMemberTestCase):
    def setUp(self):
        self.lock = self.client.get_lock(random_string()).blocking()

    def tearDown(self):
        self.lock.destroy()

    def test_lock(self):
        self.lock.lock(10)

        e = Event()

        def lock():
            if not self.lock.try_lock():
                e.set()

        self.start_new_thread(lock)
        self.assertSetEventually(e)

    def test_lock_with_lease(self):
        self.lock.lock(lease_time=1)

        e = Event()

        def lock():
            if self.lock.try_lock(timeout=10):
                e.set()

        self.start_new_thread(lock)
        self.assertSetEventually(e)

    def test_is_locked_when_not_locked(self):
        self.assertFalse(self.lock.is_locked())

    def test_is_locked_when_locked(self):
        self.lock.lock()
        self.assertTrue(self.lock.is_locked())

    def test_is_locked_when_locked_and_unlocked(self):
        self.lock.lock()
        self.lock.unlock()
        self.assertFalse(self.lock.is_locked())

    def test_is_locked_by_current_thread(self):
        self.lock.lock()
        self.assertTrue(self.lock.is_locked_by_current_thread())

    def test_is_locked_by_current_thread_with_another_thread(self):
        self.start_new_thread(lambda: self.lock.lock())

        self.assertFalse(self.lock.is_locked_by_current_thread())

    def test_get_remaining_lease_time(self):
        self.lock.lock(10)
        self.assertGreater(self.lock.get_remaining_lease_time(), 9000)

    def test_get_lock_count_single(self):
        self.lock.lock()

        self.assertEqual(self.lock.get_lock_count(), 1)

    def test_get_lock_count_when_reentrant(self):
        self.lock.lock()
        self.lock.lock()

        self.assertEqual(self.lock.get_lock_count(), 2)

    def test_force_unlock(self):
        t = self.start_new_thread(lambda: self.lock.lock())
        t.join()

        self.lock.force_unlock()

        self.assertFalse(self.lock.is_locked())

    def test_str(self):
        self.assertTrue(str(self.lock).startswith("Lock"))

    def test_key_owner_shutdowns_after_invocation_timeout(self):
        key_owner = self.cluster.start_member()

        invocation_timeout_seconds = 1
        self.client.invoker.invocation_timeout = invocation_timeout_seconds

        key = generate_key_owned_by_instance(self.client, key_owner.address)
        server_lock = self.client.get_lock(key).blocking()
        server_lock.lock()

        e = Event()

        def lock_thread_func():
            lock = self.client.get_lock(key).blocking()
            lock.lock()
            lock.unlock()
            e.set()

        self.start_new_thread(lock_thread_func)

        time.sleep(2 * invocation_timeout_seconds)
        key_owner.shutdown()

        partition_id = self.client.partition_service.get_partition_id(key)
        while not (self.client.partition_service.get_partition_owner(partition_id) == self.member.address):
            time.sleep(0.1)
        try:
            self.assertTrue(server_lock.is_locked())
            self.assertTrue(server_lock.is_locked_by_current_thread())
            self.assertTrue(server_lock.try_lock())
            server_lock.unlock()
            server_lock.unlock()
            self.assertSetEventually(e)
        finally:
            # revert the invocation timeout change for other tests since client instance is only created once.
            self.client.invoker.invocation_timeout = self.client.properties.INVOCATION_TIMEOUT_SECONDS.default_value
