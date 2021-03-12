import time

from hazelcast import HazelcastClient
from hazelcast.errors import (
    DistributedObjectDestroyedError,
    IllegalMonitorStateError,
)
from hazelcast.proxy.cp.fenced_lock import FencedLock
from tests.integration.proxy.cp import CPTestCase
from tests.util import random_string


class FencedLockTest(CPTestCase):
    def setUp(self):
        self.lock = self.client.cp_subsystem.get_lock(random_string()).blocking()
        self.initial_acquire_count = self.get_initial_acquire_count()

    def tearDown(self):
        self.lock.destroy()

    def test_lock_in_another_group(self):
        another_lock = self.client.cp_subsystem.get_lock(
            self.lock._proxy_name + "@another"
        ).blocking()
        self.assert_valid_fence(another_lock.lock())
        try:
            self.assertTrue(another_lock.is_locked())
            self.assertFalse(self.lock.is_locked())
        finally:
            another_lock.unlock()

    def test_lock_after_client_shutdown(self):
        another_client = HazelcastClient(cluster_name=self.cluster.id)
        another_lock = another_client.cp_subsystem.get_lock(self.lock._proxy_name).blocking()
        self.assert_valid_fence(another_lock.lock())
        self.assertTrue(another_lock.is_locked())
        self.assertTrue(self.lock.is_locked())
        another_client.shutdown()

        def assertion():
            self.assertFalse(self.lock.is_locked())

        self.assertTrueEventually(assertion)

    def test_use_after_destroy(self):
        self.lock.destroy()
        # the next destroy call should be ignored
        self.lock.destroy()

        with self.assertRaises(DistributedObjectDestroyedError):
            self.lock.lock()

        lock2 = self.client.cp_subsystem.get_lock(self.lock._proxy_name).blocking()

        with self.assertRaises(DistributedObjectDestroyedError):
            lock2.lock()

    def test_lock_when_not_locked(self):
        self.assert_valid_fence(self.lock.lock())
        self.assert_lock(True, True, 1)
        self.assert_session_acquire_count(1)

    def test_lock_when_locked_by_self(self):
        fence = self.lock.lock()
        self.assert_valid_fence(fence)
        fence2 = self.lock.lock()
        self.assertEqual(fence, fence2)
        self.assert_lock(True, True, 2)
        self.assert_session_acquire_count(2)
        self.lock.unlock()
        self.lock.unlock()
        self.assert_session_acquire_count(0)
        fence3 = self.lock.lock()
        self.assert_valid_fence(fence3)
        self.assertGreater(fence3, fence)
        self.assert_session_acquire_count(1)

    def test_lock_when_locked_by_another_thread(self):
        t = self.start_new_thread(lambda: self.assert_valid_fence(self.lock.lock()))
        t.join()
        self.assert_lock(True, False, 1)
        future = self.lock._wrapped.lock()
        time.sleep(2)
        self.assertFalse(future.done())  # lock request made in the main thread should not complete
        self.assert_session_acquire_count(2)
        self.lock.destroy()

        with self.assertRaises(DistributedObjectDestroyedError):
            future.result()

    def test_try_lock_when_free(self):
        self.assert_valid_fence(self.lock.try_lock())
        self.assert_lock(True, True, 1)
        self.assert_session_acquire_count(1)

    def test_try_lock_when_free_with_timeout(self):
        self.assert_valid_fence(self.lock.try_lock(1))
        self.assert_lock(True, True, 1)
        self.assert_session_acquire_count(1)

    def test_try_lock_when_locked_by_self(self):
        fence = self.lock.lock()
        self.assert_valid_fence(fence)
        fence2 = self.lock.try_lock()
        self.assertEqual(fence, fence2)
        self.assert_lock(True, True, 2)
        self.assert_session_acquire_count(2)
        self.lock.unlock()
        self.lock.unlock()
        self.assert_session_acquire_count(0)
        fence3 = self.lock.try_lock()
        self.assert_valid_fence(fence3)
        self.assertGreater(fence3, fence)
        self.assert_session_acquire_count(1)

    def test_try_lock_when_locked_by_self_with_timeout(self):
        fence = self.lock.lock()
        self.assert_valid_fence(fence)
        fence2 = self.lock.try_lock(2)
        self.assertEqual(fence, fence2)
        self.assert_lock(True, True, 2)
        self.assert_session_acquire_count(2)
        self.lock.unlock()
        self.lock.unlock()
        self.assert_session_acquire_count(0)
        fence3 = self.lock.try_lock(2)
        self.assert_valid_fence(fence3)
        self.assertGreater(fence3, fence)
        self.assert_session_acquire_count(1)

    def test_try_lock_when_locked_by_another_thread(self):
        t = self.start_new_thread(lambda: self.lock.try_lock())
        t.join()
        self.assertFalse(self.lock.try_lock())
        self.assert_lock(True, False, 1)
        self.assert_session_acquire_count(1)

    def test_try_lock_when_locked_by_another_thread_with_timeout(self):
        t = self.start_new_thread(lambda: self.assert_valid_fence(self.lock.try_lock()))
        t.join()
        self.assert_not_valid_fence(self.lock.try_lock(1))
        self.assert_lock(True, False, 1)
        self.assert_session_acquire_count(1)

    def test_unlock_when_free(self):
        with self.assertRaises(IllegalMonitorStateError):
            self.lock.unlock()

        self.assert_lock(False, False, 0)
        self.assert_session_acquire_count(0)

    def test_unlock_when_locked_by_self(self):
        self.lock.lock()
        self.assertIsNone(self.lock.unlock())

        with self.assertRaises(IllegalMonitorStateError):
            self.lock.unlock()

        self.assert_lock(False, False, 0)
        self.assert_session_acquire_count(0)

    def test_unlock_multiple_times(self):
        self.lock.lock()
        self.lock.lock()
        self.lock.unlock()
        self.lock.unlock()
        self.assert_lock(False, False, 0)
        self.assert_session_acquire_count(0)

    def test_unlock_when_locked_by_another_thread(self):
        t = self.start_new_thread(lambda: self.lock.lock())
        t.join()

        with self.assertRaises(IllegalMonitorStateError):
            self.lock.unlock()

        self.assert_lock(True, False, 1)
        self.assert_session_acquire_count(1)

    def test_is_locked_when_free(self):
        self.assertFalse(self.lock.is_locked())
        self.assert_session_acquire_count(0)

    def test_is_locked_when_locked_by_self(self):
        self.lock.lock()
        self.assertTrue(self.lock.is_locked())
        self.lock.unlock()
        self.assertFalse(self.lock.is_locked())
        self.assert_session_acquire_count(0)

    def test_is_locked_when_locked_by_another_thread(self):
        t = self.start_new_thread(lambda: self.lock.lock())
        t.join()

        self.assertTrue(self.lock.is_locked())
        self.assert_session_acquire_count(1)

    def test_is_locked_by_current_thread_when_free(self):
        self.assertFalse(self.lock.is_locked_by_current_thread())
        self.assert_session_acquire_count(0)

    def test_is_locked_by_current_thread_when_locked_by_self(self):
        self.lock.lock()
        self.assertTrue(self.lock.is_locked_by_current_thread())
        self.lock.unlock()
        self.assertFalse(self.lock.is_locked_by_current_thread())
        self.assert_session_acquire_count(0)

    def test_is_locked_by_current_thread_when_locked_by_another_thread(self):
        t = self.start_new_thread(lambda: self.lock.lock())
        t.join()

        self.assertFalse(self.lock.is_locked_by_current_thread())
        self.assert_session_acquire_count(1)

    def assert_lock(self, is_locked, is_locked_by_current_thread, lock_count):
        self.assertEqual(is_locked, self.lock.is_locked())
        self.assertEqual(is_locked_by_current_thread, self.lock.is_locked_by_current_thread())
        self.assertEqual(lock_count, self.lock.get_lock_count())

    def assert_valid_fence(self, fence):
        self.assertNotEqual(FencedLock.INVALID_FENCE, fence)

    def assert_not_valid_fence(self, fence):
        self.assertEqual(FencedLock.INVALID_FENCE, fence)

    def assert_session_acquire_count(self, expected_acquire_count):
        session = self.client._proxy_session_manager._sessions.get(self.lock.get_group_id(), None)
        if not session:
            actual = 0
        else:
            actual = session.acquire_count.get()

        self.assertEqual(self.initial_acquire_count + expected_acquire_count, actual)

    def get_initial_acquire_count(self):
        session = self.client._proxy_session_manager._sessions.get(self.lock.get_group_id(), None)
        if not session:
            return 0
        return session.acquire_count.get()
