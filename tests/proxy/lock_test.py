from threading import Event

from tests.base import SingleMemberTestCase
from tests.util import random_string


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