import time
import unittest

from mock import MagicMock

from hazelcast import HazelcastClient
from hazelcast.cp import LOCK_SERVICE
from hazelcast.errors import DistributedObjectDestroyedError, IllegalMonitorStateError, \
    LockOwnershipLostError, LockAcquireLimitReachedError, SessionExpiredError, WaitKeyCancelledError, \
    HazelcastRuntimeError
from hazelcast.future import ImmediateFuture, ImmediateExceptionFuture
from hazelcast.protocol import RaftGroupId
from hazelcast.proxy.cp.fenced_lock import FencedLock
from hazelcast.util import AtomicInteger, thread_id
from tests.proxy.cp import CPTestCase
from tests.util import random_string


class FencedLockTest(CPTestCase):
    def setUp(self):
        self.lock = self.client.cp_subsystem.get_lock(random_string()).blocking()
        self.initial_acquire_count = self.get_initial_acquire_count()

    def tearDown(self):
        self.lock.destroy()

    def test_lock_in_another_group(self):
        another_lock = self.client.cp_subsystem.get_lock(self.lock._proxy_name + "@another").blocking()
        another_lock.lock()
        try:
            self.assertTrue(another_lock.is_locked())
            self.assertFalse(self.lock.is_locked())
        finally:
            another_lock.unlock()

    def test_lock_after_client_shutdown(self):
        another_client = HazelcastClient(cluster_name=self.cluster.id)
        another_lock = another_client.cp_subsystem.get_lock(self.lock._proxy_name).blocking()
        another_lock.lock()
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
            self.lock.lock_and_get_fence()

        lock2 = self.client.cp_subsystem.get_lock(self.lock._proxy_name).blocking()

        with self.assertRaises(DistributedObjectDestroyedError):
            lock2.lock_and_get_fence()

    def test_lock_when_not_locked(self):
        self.assertIsNone(self.lock.lock())
        self.assertTrue(self.lock.is_locked())
        self.assertTrue(self.lock.is_locked_by_current_thread())
        self.assertEqual(1, self.lock.get_lock_count())
        self.assert_session_acquire_count(1)

    def test_lock_when_locked_by_self(self):
        self.lock.lock()
        self.lock.lock()
        self.assert_lock(True, True, 2)
        self.assert_session_acquire_count(2)

    def test_lock_when_locked_by_another_thread(self):
        t = self.start_new_thread(lambda: self.lock.lock())
        t.join()
        self.assert_lock(True, False, 1)
        future = self.lock._wrapped.lock()
        time.sleep(2)
        self.assertFalse(future.done())  # lock request made in the main thread should not complete
        self.assert_session_acquire_count(2)
        self.lock.destroy()

        with self.assertRaises(DistributedObjectDestroyedError):
            future.result()

    def test_lock_and_get_fence_when_not_locked(self):
        self.assert_valid_fence(self.lock.lock_and_get_fence())
        self.assert_lock(True, True, 1)
        self.assert_session_acquire_count(1)

    def test_lock_and_get_fence_when_locked_by_self(self):
        fence = self.lock.lock_and_get_fence()
        self.assert_valid_fence(fence)
        fence2 = self.lock.lock_and_get_fence()
        self.assertEqual(fence, fence2)
        self.assert_lock(True, True, 2)
        self.assert_session_acquire_count(2)
        self.lock.unlock()
        self.lock.unlock()
        self.assert_session_acquire_count(0)
        fence3 = self.lock.lock_and_get_fence()
        self.assert_valid_fence(fence3)
        self.assertGreater(fence3, fence)
        self.assert_session_acquire_count(1)

    def test_lock_and_get_fence_when_locked_by_another_thread(self):
        t = self.start_new_thread(lambda: self.assert_valid_fence(self.lock.lock_and_get_fence()))
        t.join()
        self.assert_lock(True, False, 1)
        future = self.lock._wrapped.lock_and_get_fence()
        time.sleep(2)
        self.assertFalse(future.done())  # lock request made in the main thread should not complete
        self.assert_session_acquire_count(2)
        self.lock.destroy()

        with self.assertRaises(DistributedObjectDestroyedError):
            future.result()

    def test_try_lock_when_free(self):
        self.assertTrue(self.lock.try_lock())
        self.assert_lock(True, True, 1)
        self.assert_session_acquire_count(1)

    def test_try_lock_when_free_with_timeout(self):
        self.assertTrue(self.lock.try_lock(1))
        self.assert_lock(True, True, 1)
        self.assert_session_acquire_count(1)

    def test_try_lock_when_locked_by_self(self):
        self.lock.lock()
        self.assertTrue(self.lock.try_lock())
        self.assert_lock(True, True, 2)
        self.assert_session_acquire_count(2)

    def test_try_lock_when_locked_by_self_with_timeout(self):
        self.lock.lock()
        self.assertTrue(self.lock.try_lock(2))
        self.assert_lock(True, True, 2)
        self.assert_session_acquire_count(2)

    def test_try_lock_when_locked_by_another_thread(self):
        t = self.start_new_thread(lambda: self.lock.try_lock())
        t.join()
        self.assertFalse(self.lock.try_lock())
        self.assert_lock(True, False, 1)
        self.assert_session_acquire_count(1)

    def test_try_lock_when_locked_by_another_thread_with_timeout(self):
        t = self.start_new_thread(lambda: self.lock.try_lock())
        t.join()
        self.assertFalse(self.lock.try_lock(1))
        self.assert_lock(True, False, 1)
        self.assert_session_acquire_count(1)

    def test_try_lock_and_get_fence_when_free(self):
        self.assert_valid_fence(self.lock.try_lock_and_get_fence())
        self.assert_lock(True, True, 1)
        self.assert_session_acquire_count(1)

    def test_try_lock_and_get_fence_when_free_with_timeout(self):
        self.assert_valid_fence(self.lock.try_lock(1))
        self.assert_lock(True, True, 1)
        self.assert_session_acquire_count(1)

    def test_try_lock_and_get_fence_when_locked_by_self(self):
        fence = self.lock.try_lock_and_get_fence()
        self.assert_valid_fence(fence)
        fence2 = self.lock.try_lock_and_get_fence()
        self.assertEqual(fence, fence2)
        self.assert_lock(True, True, 2)
        self.assert_session_acquire_count(2)
        self.lock.unlock()
        self.lock.unlock()
        self.assert_session_acquire_count(0)
        fence3 = self.lock.try_lock_and_get_fence()
        self.assert_valid_fence(fence3)
        self.assertGreater(fence3, fence)
        self.assert_session_acquire_count(1)

    def test_try_lock_and_get_fence_when_locked_by_self_with_timeout(self):
        fence = self.lock.try_lock_and_get_fence(1)
        self.assert_valid_fence(fence)
        fence2 = self.lock.try_lock_and_get_fence(2)
        self.assertEqual(fence, fence2)
        self.assert_lock(True, True, 2)
        self.assert_session_acquire_count(2)
        self.lock.unlock()
        self.lock.unlock()
        self.assert_session_acquire_count(0)
        fence3 = self.lock.try_lock_and_get_fence()
        self.assert_valid_fence(fence3)
        self.assertGreater(fence3, fence)
        self.assert_session_acquire_count(1)

    def test_try_lock_and_get_fence_when_locked_by_another_thread(self):
        t = self.start_new_thread(lambda: self.assert_valid_fence(self.lock.try_lock_and_get_fence()))
        t.join()
        self.assert_not_valid_fence(self.lock.try_lock_and_get_fence())
        self.assert_lock(True, False, 1)
        self.assert_session_acquire_count(1)

    def test_try_lock_and_get_fence_when_locked_by_another_thread_with_timeout(self):
        t = self.start_new_thread(lambda: self.assert_valid_fence(self.lock.try_lock_and_get_fence(1)))
        t.join()
        self.assert_not_valid_fence(self.lock.try_lock_and_get_fence(1))
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

    def test_get_fence_when_free(self):
        with self.assertRaises(IllegalMonitorStateError):
            self.lock.get_fence()

        self.assert_session_acquire_count(0)

    def test_get_fence_when_locked_by_self(self):
        self.lock.lock()
        fence = self.lock.get_fence()
        self.assert_valid_fence(fence)
        self.lock.lock()
        fence2 = self.lock.get_fence()
        self.assertEqual(fence, fence2)
        self.lock.unlock()
        self.lock.unlock()
        self.lock.lock()
        fence3 = self.lock.get_fence()
        self.assert_valid_fence(fence3)
        self.assertGreater(fence3, fence)
        self.lock.unlock()

        with self.assertRaises(IllegalMonitorStateError):
            self.lock.get_fence()

        self.assert_session_acquire_count(0)

    def test_get_fence_when_locked_by_another_thread(self):
        t = self.start_new_thread(lambda: self.lock.lock())
        t.join()

        with self.assertRaises(IllegalMonitorStateError):
            self.lock.get_fence()

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


class FencedLockMockTest(unittest.TestCase):
    def setUp(self):
        self.acquire_session = MagicMock()
        self.release_session = MagicMock()
        self.invalidate_session = MagicMock()
        self.session_manager = MagicMock(acquire_session=self.acquire_session, release_session=self.release_session,
                                         invalidate_session=self.invalidate_session)
        context = MagicMock(proxy_session_manager=self.session_manager)
        group_id = RaftGroupId("test", 0, 42)
        self.proxy = FencedLock(context, group_id, LOCK_SERVICE, "mylock@mygroup", "mylock").blocking()

    def test_lock(self):
        # Everything succeeds
        self.prepare_acquire_session(1)
        self.mock_request_lock(2)
        self.proxy.lock()
        self.assert_call_counts(1, 0, 0)
        self.assert_lock_session_id(1)

    def test_lock_when_acquire_session_fails(self):
        # First call to acquire session fails, should not retry
        self.prepare_acquire_session(-1, HazelcastRuntimeError("server_error"))

        with self.assertRaises(HazelcastRuntimeError):
            self.proxy.lock()

        self.assert_call_counts(1, 0, 0)
        self.assert_no_lock_session_id()

    def test_lock_when_server_closes_old_session(self):
        # Same thread issues a new lock call while holding a lock.
        # Server closes session related to the first lock, should not retry
        self.prepare_acquire_session(2)
        self.prepare_lock_session_ids(1)

        with self.assertRaises(LockOwnershipLostError):
            self.proxy.lock()

        self.assert_call_counts(1, 1, 0)
        self.assert_no_lock_session_id()

    def test_lock_when_lock_acquire_limit_reached(self):
        # Lock acquire limit is reached, server returns invalid fence, should not retry
        self.prepare_acquire_session(1)
        self.mock_request_lock(FencedLock.INVALID_FENCE)

        with self.assertRaises(LockAcquireLimitReachedError):
            self.proxy.lock()

        self.assert_call_counts(1, 1, 0)
        self.assert_no_lock_session_id()

    def test_lock_on_session_expired_error(self):
        # Session expired error comes from the server on lock request, retries and gets valid fence
        self.prepare_acquire_session(1)
        self.mock_request_lock(2, SessionExpiredError())
        self.proxy.lock()
        self.assert_call_counts(2, 0, 1)
        self.assert_lock_session_id(1)

    def test_lock_on_session_expired_error_on_reentrant_lock_request(self):
        # Session expired error comes from the server on second lock request,
        # while holding a lock, should not retry
        self.prepare_acquire_session(1)
        self.prepare_lock_session_ids(1)
        self.mock_request_lock(3, SessionExpiredError())

        with self.assertRaises(LockOwnershipLostError):
            self.proxy.lock()

        self.assert_call_counts(1, 0, 1)
        self.assert_no_lock_session_id()

    def test_lock_on_wait_key_cancelled_error(self):
        # Wait key cancelled error comes from the server, should not retry
        self.prepare_acquire_session(1)
        self.mock_request_lock(2, WaitKeyCancelledError())

        with self.assertRaises(IllegalMonitorStateError):
            self.proxy.lock()

        self.assert_call_counts(1, 1, 0)
        self.assert_no_lock_session_id()

    def test_lock_on_unspecified_error(self):
        # Server sends another error, should not retry
        self.prepare_acquire_session(1)
        self.mock_request_lock(-1, HazelcastRuntimeError("expected"))

        with self.assertRaises(HazelcastRuntimeError):
            self.proxy.lock()

        self.assert_call_counts(1, 1, 0)
        self.assert_no_lock_session_id()

    def test_lock_and_get_fence(self):
        # Everything succeeds
        self.prepare_acquire_session(1)
        self.mock_request_lock(2)
        self.assertEqual(2, self.proxy.lock_and_get_fence())
        self.assert_call_counts(1, 0, 0)
        self.assert_lock_session_id(1)

    def test_lock_and_get_fence_when_acquire_session_fails(self):
        # First call to acquire session fails, should not retry
        self.prepare_acquire_session(-1, HazelcastRuntimeError("server_error"))

        with self.assertRaises(HazelcastRuntimeError):
            self.proxy.lock_and_get_fence()

        self.assert_call_counts(1, 0, 0)
        self.assert_no_lock_session_id()

    def test_lock_and_get_fence_when_server_closes_old_session(self):
        # Same thread issues a new lock call while holding a lock.
        # Server closes session related to the first lock, should not retry
        self.prepare_acquire_session(2)
        self.prepare_lock_session_ids(1)

        with self.assertRaises(LockOwnershipLostError):
            self.proxy.lock_and_get_fence()

        self.assert_call_counts(1, 1, 0)
        self.assert_no_lock_session_id()

    def test_lock_and_get_fence_when_lock_acquire_limit_reached(self):
        # Lock acquire limit is reached, server returns invalid fence, should not retry
        self.prepare_acquire_session(1)
        self.mock_request_lock(FencedLock.INVALID_FENCE)

        with self.assertRaises(LockAcquireLimitReachedError):
            self.proxy.lock_and_get_fence()

        self.assert_call_counts(1, 1, 0)
        self.assert_no_lock_session_id()

    def test_lock_and_get_fence_on_session_expired_error(self):
        # Session expired error comes from the server on lock request, retries and gets valid fence
        self.prepare_acquire_session(1)
        self.mock_request_lock(2, SessionExpiredError())
        self.assertEqual(2, self.proxy.lock_and_get_fence())
        self.assert_call_counts(2, 0, 1)
        self.assert_lock_session_id(1)

    def test_lock_and_get_fence_on_session_expired_error_on_reentrant_lock_request(self):
        # Session expired error comes from the server on second lock request,
        # while holding a lock, should not retry
        self.prepare_acquire_session(1)
        self.prepare_lock_session_ids(1)
        self.mock_request_lock(3, SessionExpiredError())

        with self.assertRaises(LockOwnershipLostError):
            self.proxy.lock_and_get_fence()

        self.assert_call_counts(1, 0, 1)
        self.assert_no_lock_session_id()

    def test_lock_and_get_fence_on_wait_key_cancelled_error(self):
        # Wait key cancelled error comes from the server, should not retry
        self.prepare_acquire_session(1)
        self.mock_request_lock(2, WaitKeyCancelledError())

        with self.assertRaises(IllegalMonitorStateError):
            self.proxy.lock_and_get_fence()

        self.assert_call_counts(1, 1, 0)
        self.assert_no_lock_session_id()

    def test_lock_and_get_fence_on_unspecified_error(self):
        # Server sends another error, should not retry
        self.prepare_acquire_session(1)
        self.mock_request_lock(-1, HazelcastRuntimeError("expected"))

        with self.assertRaises(HazelcastRuntimeError):
            self.proxy.lock_and_get_fence()

        self.assert_call_counts(1, 1, 0)
        self.assert_no_lock_session_id()

    def test_try_lock(self):
        # Everything succeeds
        self.prepare_acquire_session(1)
        self.mock_request_try_lock(2)
        self.assertTrue(self.proxy.try_lock())
        self.assert_call_counts(1, 0, 0)
        self.assert_lock_session_id(1)

    def test_try_lock_when_acquire_session_fails(self):
        # First call to acquire session fails, should not retry
        self.prepare_acquire_session(-1, HazelcastRuntimeError("server_error"))

        with self.assertRaises(HazelcastRuntimeError):
            self.proxy.try_lock()

        self.assert_call_counts(1, 0, 0)
        self.assert_no_lock_session_id()

    def test_try_lock_when_server_closes_old_session(self):
        # Same thread issues a new lock call while holding a lock.
        # Server closes session related to the first lock, should not retry
        self.prepare_acquire_session(2)
        self.prepare_lock_session_ids(1)

        with self.assertRaises(LockOwnershipLostError):
            self.proxy.try_lock()

        self.assert_call_counts(1, 1, 0)
        self.assert_no_lock_session_id()

    def test_try_lock_when_lock_acquire_limit_reached(self):
        # Lock acquire limit is reached, server returns invalid fence
        self.prepare_acquire_session(1)
        self.mock_request_try_lock(FencedLock.INVALID_FENCE)
        self.assertFalse(self.proxy.try_lock())
        self.assert_call_counts(1, 1, 0)
        self.assert_no_lock_session_id()

    def test_try_lock_on_session_expired_error(self):
        # Session expired error comes from the server on lock request,
        # client determines the timeout and returns invalid fence
        self.prepare_acquire_session(1)
        self.mock_request_try_lock(2, SessionExpiredError())
        self.assertFalse(self.proxy.try_lock())
        self.assert_call_counts(1, 0, 1)
        self.assert_no_lock_session_id()

    def test_try_lock_on_session_expired_error_when_not_timed_out(self):
        # Session expired error comes from the server on lock request,
        # client retries due to not reaching timeout and succeeds
        self.prepare_acquire_session(1)
        self.mock_request_try_lock(2, SessionExpiredError())
        self.assertTrue(self.proxy.try_lock(100))
        self.assert_call_counts(2, 0, 1)
        self.assert_lock_session_id(1)

    def test_try_lock_on_session_expired_error_on_reentrant_lock_request(self):
        # Session expired error comes from the server on second lock request,
        # while holding a lock, should not retry
        self.prepare_acquire_session(1)
        self.prepare_lock_session_ids(1)
        self.mock_request_try_lock(3, SessionExpiredError())

        with self.assertRaises(LockOwnershipLostError):
            self.proxy.try_lock()

        self.assert_call_counts(1, 0, 1)
        self.assert_no_lock_session_id()

    def test_try_lock_on_wait_key_cancelled_error(self):
        # Wait key cancelled error comes from the server, invalid fence is returned
        self.prepare_acquire_session(1)
        self.mock_request_try_lock(2, WaitKeyCancelledError())
        self.assertFalse(self.proxy.try_lock())
        self.assert_call_counts(1, 1, 0)
        self.assert_no_lock_session_id()

    def test_try_lock_on_unspecified_error(self):
        # Server sends another error, should not retry
        self.prepare_acquire_session(1)
        self.mock_request_try_lock(-1, HazelcastRuntimeError("expected"))

        with self.assertRaises(HazelcastRuntimeError):
            self.proxy.try_lock()

        self.assert_call_counts(1, 1, 0)
        self.assert_no_lock_session_id()

    def test_try_lock_and_get_fence(self):
        # Everything succeeds
        self.prepare_acquire_session(1)
        self.mock_request_try_lock(2)
        self.assertEqual(2, self.proxy.try_lock_and_get_fence())
        self.assert_call_counts(1, 0, 0)
        self.assert_lock_session_id(1)

    def test_try_lock_and_get_fence_when_acquire_session_fails(self):
        # First call to acquire session fails, should not retry
        self.prepare_acquire_session(-1, HazelcastRuntimeError("server_error"))

        with self.assertRaises(HazelcastRuntimeError):
            self.proxy.try_lock_and_get_fence()

        self.assert_call_counts(1, 0, 0)
        self.assert_no_lock_session_id()

    def test_try_lock_and_get_fence_when_server_closes_old_session(self):
        # Same thread issues a new lock call while holding a lock.
        # Server closes session related to the first lock, should not retry
        self.prepare_acquire_session(2)
        self.prepare_lock_session_ids(1)

        with self.assertRaises(LockOwnershipLostError):
            self.proxy.try_lock_and_get_fence()

        self.assert_call_counts(1, 1, 0)
        self.assert_no_lock_session_id()

    def test_try_lock_and_get_fence_when_lock_acquire_limit_reached(self):
        # Lock acquire limit is reached, server returns invalid fence
        self.prepare_acquire_session(1)
        self.mock_request_try_lock(FencedLock.INVALID_FENCE)
        self.assertEqual(FencedLock.INVALID_FENCE, self.proxy.try_lock_and_get_fence())
        self.assert_call_counts(1, 1, 0)
        self.assert_no_lock_session_id()

    def test_try_lock_and_get_fence_on_session_expired_error(self):
        # Session expired error comes from the server on lock request,
        # client determines the timeout and returns invalid fence
        self.prepare_acquire_session(1)
        self.mock_request_try_lock(2, SessionExpiredError())
        self.assertEqual(FencedLock.INVALID_FENCE, self.proxy.try_lock_and_get_fence())
        self.assert_call_counts(1, 0, 1)
        self.assert_no_lock_session_id()

    def test_try_lock_and_get_fence_on_session_expired_error_when_not_timed_out(self):
        # Session expired error comes from the server on lock request,
        # client retries due to not reaching timeout and succeeds
        self.prepare_acquire_session(1)
        self.mock_request_try_lock(2, SessionExpiredError())
        self.assertEqual(2, self.proxy.try_lock_and_get_fence(100))
        self.assert_call_counts(2, 0, 1)
        self.assert_lock_session_id(1)

    def test_try_lock_and_get_fence_on_session_expired_error_on_reentrant_lock_request(self):
        # Session expired error comes from the server on second lock request,
        # while holding a lock, should not retry
        self.prepare_acquire_session(1)
        self.prepare_lock_session_ids(1)
        self.mock_request_try_lock(3, SessionExpiredError())

        with self.assertRaises(LockOwnershipLostError):
            self.proxy.try_lock_and_get_fence()

        self.assert_call_counts(1, 0, 1)
        self.assert_no_lock_session_id()

    def test_try_lock_and_get_fence_on_wait_key_cancelled_error(self):
        # Wait key cancelled error comes from the server, invalid fence is returned
        self.prepare_acquire_session(1)
        self.mock_request_try_lock(2, WaitKeyCancelledError())
        self.assertEqual(FencedLock.INVALID_FENCE, self.proxy.try_lock_and_get_fence())
        self.assert_call_counts(1, 1, 0)
        self.assert_no_lock_session_id()

    def test_try_lock_and_get_fence_on_unspecified_error(self):
        # Server sends another error, should not retry
        self.prepare_acquire_session(1)
        self.mock_request_try_lock(-1, HazelcastRuntimeError("expected"))

        with self.assertRaises(HazelcastRuntimeError):
            self.proxy.try_lock_and_get_fence()

        self.assert_call_counts(1, 1, 0)
        self.assert_no_lock_session_id()

    def test_unlock(self):
        # Everything succeeds
        self.prepare_get_session(2)
        self.mock_request_unlock(True)
        self.proxy.unlock()
        self.assert_call_counts(0, 1, 0)
        self.assert_lock_session_id(2)  # Server sent true, client still holds the lock after unlock

    def test_unlock_when_server_closes_old_session(self):
        # Session id is different than what we store in the
        # dict. The old session must be closed while we were
        # holding the lock.
        self.prepare_get_session(2)
        self.prepare_lock_session_ids(1)

        with self.assertRaises(LockOwnershipLostError):
            self.proxy.unlock()

        self.assert_call_counts(0, 0, 0)
        self.assert_no_lock_session_id()

    def test_unlock_when_there_is_no_session(self):
        # No active session for the current thread.
        self.prepare_get_session(-1)

        with self.assertRaises(IllegalMonitorStateError):
            self.proxy.unlock()

        self.assert_call_counts(0, 0, 0)
        self.assert_no_lock_session_id()

    def test_unlock_when_client_unlocked_the_locked(self):
        # After the unlock, lock is free
        self.prepare_get_session(1)
        self.mock_request_unlock(False)
        self.proxy.unlock()
        self.assert_call_counts(0, 1, 0)
        self.assert_no_lock_session_id()

    def test_unlock_on_session_expired_error(self):
        # Server sends session expired error
        self.prepare_get_session(1)
        self.mock_request_unlock(None, SessionExpiredError())

        with self.assertRaises(LockOwnershipLostError):
            self.proxy.unlock()

        self.assert_call_counts(0, 0, 1)
        self.assert_no_lock_session_id()

    def test_unlock_on_illegal_monitor_state_error(self):
        # Lock is not held by the current thread, but client
        # thinks that it holds it and sends the request.
        # Server sends illegal monitor state error in response.
        self.prepare_get_session(1)
        self.mock_request_unlock(None, IllegalMonitorStateError())

        with self.assertRaises(IllegalMonitorStateError):
            self.proxy.unlock()

        self.assert_call_counts(0, 0, 0)
        self.assert_no_lock_session_id()

    def test_unlock_on_unspecified_error(self):
        # Server sends an unspecified error
        self.prepare_get_session(1)
        self.mock_request_unlock(None, HazelcastRuntimeError())

        with self.assertRaises(HazelcastRuntimeError):
            self.proxy.unlock()

        self.assert_call_counts(0, 0, 0)
        self.assert_no_lock_session_id()

    def test_get_fence(self):
        # Everything succeeds
        self.prepare_get_session(2)
        state = self.prepare_state(3, 1, 2, thread_id())
        self.mock_request_get_lock_ownership_state(state)
        self.assertEqual(3, self.proxy.get_fence())
        self.assert_call_counts(0, 0, 0)
        self.assert_lock_session_id(2)
        
    def test_get_fence_when_server_closes_old_session(self):
        # Session id is different than what we store in the
        # dict. The old session must be closed while we were
        # holding the lock.
        self.prepare_get_session(2)
        self.prepare_lock_session_ids(1)

        with self.assertRaises(LockOwnershipLostError):
            self.proxy.get_fence()

        self.assert_call_counts(0, 0, 0)
        self.assert_no_lock_session_id()

    def test_get_fence_when_there_is_no_session(self):
        # No active session for the current thread.
        self.prepare_get_session(-1)

        with self.assertRaises(IllegalMonitorStateError):
            self.proxy.get_fence()

        self.assert_call_counts(0, 0, 0)
        self.assert_no_lock_session_id()

    def test_get_fence_when_server_returns_a_different_thread_id_for_lock_holder(self):
        # Client thinks that it holds the lock, but server
        # says it's not.
        self.prepare_get_session(1)
        self.prepare_lock_session_ids(1)
        state = self.prepare_state(3, 1, 2, thread_id() - 1)
        self.mock_request_get_lock_ownership_state(state)

        with self.assertRaises(LockOwnershipLostError):
            self.proxy.get_fence()

        self.assert_call_counts(0, 0, 0)
        self.assert_no_lock_session_id()

    def test_get_fence_when_not_holding_the_lock(self):
        # Client is not holding the lock.
        self.prepare_get_session(1)
        state = self.prepare_state(3, 1, 2, thread_id() - 1)
        self.mock_request_get_lock_ownership_state(state)

        with self.assertRaises(IllegalMonitorStateError):
            self.proxy.get_fence()

        self.assert_call_counts(0, 0, 0)
        self.assert_no_lock_session_id()

    def test_get_fence_on_unspecified_error(self):
        # Server sends an unspecified error
        self.prepare_get_session(1)
        self.mock_request_get_lock_ownership_state(None, HazelcastRuntimeError())

        with self.assertRaises(HazelcastRuntimeError):
            self.proxy.get_fence()

        self.assert_call_counts(0, 0, 0)
        self.assert_no_lock_session_id()

    def test_is_locked(self):
        # Everything succeeds, client holds the lock
        self.prepare_get_session(2)
        state = self.prepare_state(3, 1, 2, thread_id())
        self.mock_request_get_lock_ownership_state(state)
        self.assertTrue(self.proxy.is_locked())
        self.assert_call_counts(0, 0, 0)
        self.assert_lock_session_id(2)

    def test_is_locked_when_it_is_locked_by_another_thread(self):
        # Client is not holding the lock, but someone else does.
        self.prepare_get_session(1)
        state = self.prepare_state(3, 1, 2, thread_id() - 1)
        self.mock_request_get_lock_ownership_state(state)
        self.assertTrue(self.proxy.is_locked())
        self.assert_call_counts(0, 0, 0)
        self.assert_no_lock_session_id()

    def test_is_locked_when_free(self):
        # No one holds the lock
        self.prepare_get_session(1)
        state = self.prepare_state(FencedLock.INVALID_FENCE, 0, -1, -1)
        self.mock_request_get_lock_ownership_state(state)
        self.assertFalse(self.proxy.is_locked())
        self.assert_call_counts(0, 0, 0)
        self.assert_no_lock_session_id()

    def test_is_locked_when_server_closes_old_session(self):
        # Session id is different than what we store in the
        # dict. The old session must be closed while we were
        # holding the lock.
        self.prepare_get_session(2)
        self.prepare_lock_session_ids(1)

        with self.assertRaises(LockOwnershipLostError):
            self.proxy.is_locked()

        self.assert_call_counts(0, 0, 0)
        self.assert_no_lock_session_id()

    def test_is_locked_when_server_returns_a_different_thread_id_for_lock_holder(self):
        # Client thinks that it holds the lock, but server
        # says it's not.
        self.prepare_get_session(1)
        self.prepare_lock_session_ids(1)
        state = self.prepare_state(3, 1, 2, thread_id() - 1)
        self.mock_request_get_lock_ownership_state(state)

        with self.assertRaises(LockOwnershipLostError):
            self.proxy.is_locked()

        self.assert_call_counts(0, 0, 0)
        self.assert_no_lock_session_id()

    def test_is_locked_on_unspecified_error(self):
        # Server sends an unspecified error
        self.prepare_get_session(1)
        self.mock_request_get_lock_ownership_state(None, HazelcastRuntimeError())

        with self.assertRaises(HazelcastRuntimeError):
            self.proxy.is_locked()

        self.assert_call_counts(0, 0, 0)
        self.assert_no_lock_session_id()

    def test_is_locked_by_current_thread(self):
        # Everything succeeds, client holds the lock
        self.prepare_get_session(2)
        state = self.prepare_state(3, 1, 2, thread_id())
        self.mock_request_get_lock_ownership_state(state)
        self.assertTrue(self.proxy.is_locked_by_current_thread())
        self.assert_call_counts(0, 0, 0)
        self.assert_lock_session_id(2)

    def test_is_locked_by_current_thread_when_it_is_locked_by_another_thread(self):
        # Client is not holding the lock, but someone else does.
        self.prepare_get_session(1)
        state = self.prepare_state(3, 1, 2, thread_id() - 1)
        self.mock_request_get_lock_ownership_state(state)
        self.assertFalse(self.proxy.is_locked_by_current_thread())
        self.assert_call_counts(0, 0, 0)
        self.assert_no_lock_session_id()

    def test_is_locked_by_current_thread_when_free(self):
        # No one holds the lock
        self.prepare_get_session(1)
        state = self.prepare_state(FencedLock.INVALID_FENCE, 0, -1, -1)
        self.mock_request_get_lock_ownership_state(state)
        self.assertFalse(self.proxy.is_locked_by_current_thread())
        self.assert_call_counts(0, 0, 0)
        self.assert_no_lock_session_id()

    def test_is_locked_by_current_thread_when_server_closes_old_session(self):
        # Session id is different than what we store in the
        # dict. The old session must be closed while we were
        # holding the lock.
        self.prepare_get_session(2)
        self.prepare_lock_session_ids(1)

        with self.assertRaises(LockOwnershipLostError):
            self.proxy.is_locked_by_current_thread()

        self.assert_call_counts(0, 0, 0)
        self.assert_no_lock_session_id()

    def test_is_locked_by_current_thread_when_server_returns_a_different_thread_id_for_lock_holder(self):
        # Client thinks that it holds the lock, but server
        # says it's not.
        self.prepare_get_session(1)
        self.prepare_lock_session_ids(1)
        state = self.prepare_state(3, 1, 2, thread_id() - 1)
        self.mock_request_get_lock_ownership_state(state)

        with self.assertRaises(LockOwnershipLostError):
            self.proxy.is_locked_by_current_thread()

        self.assert_call_counts(0, 0, 0)
        self.assert_no_lock_session_id()

    def test_is_locked_by_current_thread_on_unspecified_error(self):
        # Server sends an unspecified error
        self.prepare_get_session(1)
        self.mock_request_get_lock_ownership_state(None, HazelcastRuntimeError())

        with self.assertRaises(HazelcastRuntimeError):
            self.proxy.is_locked_by_current_thread()

        self.assert_call_counts(0, 0, 0)
        self.assert_no_lock_session_id()

    def test_get_lock_count(self):
        # Everything succeeds, client holds the lock
        self.prepare_get_session(2)
        state = self.prepare_state(3, 123, 2, thread_id())
        self.mock_request_get_lock_ownership_state(state)
        self.assertEqual(123, self.proxy.get_lock_count())
        self.assert_call_counts(0, 0, 0)
        self.assert_lock_session_id(2)

    def test_get_lock_count_when_it_is_locked_by_another_thread(self):
        # Client is not holding the lock, but someone else does.
        self.prepare_get_session(1)
        state = self.prepare_state(3, 1, 2, thread_id() - 1)
        self.mock_request_get_lock_ownership_state(state)
        self.assertEqual(1, self.proxy.get_lock_count())
        self.assert_call_counts(0, 0, 0)
        self.assert_no_lock_session_id()

    def test_get_lock_count_when_free(self):
        # No one holds the lock
        self.prepare_get_session(1)
        state = self.prepare_state(FencedLock.INVALID_FENCE, 0, -1, -1)
        self.mock_request_get_lock_ownership_state(state)
        self.assertEqual(0, self.proxy.get_lock_count())
        self.assert_call_counts(0, 0, 0)
        self.assert_no_lock_session_id()

    def test_get_lock_count_when_server_closes_old_session(self):
        # Session id is different than what we store in the
        # dict. The old session must be closed while we were
        # holding the lock.
        self.prepare_get_session(2)
        self.prepare_lock_session_ids(1)

        with self.assertRaises(LockOwnershipLostError):
            self.proxy.get_lock_count()

        self.assert_call_counts(0, 0, 0)
        self.assert_no_lock_session_id()

    def test_get_lock_count_when_server_returns_a_different_thread_id_for_lock_holder(self):
        # Client thinks that it holds the lock, but server
        # says it's not.
        self.prepare_get_session(1)
        self.prepare_lock_session_ids(1)
        state = self.prepare_state(3, 1, 2, thread_id() - 1)
        self.mock_request_get_lock_ownership_state(state)

        with self.assertRaises(LockOwnershipLostError):
            self.proxy.get_lock_count()

        self.assert_call_counts(0, 0, 0)
        self.assert_no_lock_session_id()

    def test_get_lock_count_on_unspecified_error(self):
        # Server sends an unspecified error
        self.prepare_get_session(1)
        self.mock_request_get_lock_ownership_state(None, HazelcastRuntimeError())

        with self.assertRaises(HazelcastRuntimeError):
            self.proxy.get_lock_count()

        self.assert_call_counts(0, 0, 0)
        self.assert_no_lock_session_id()

    def prepare_lock_session_ids(self, session_id):
        self.proxy._wrapped._lock_session_ids[thread_id()] = session_id

    def prepare_acquire_session(self, session_id, err=None):
        if err:
            val = ImmediateExceptionFuture(err)
        else:
            val = ImmediateFuture(session_id)

        acquire_mock = MagicMock(return_value=val)
        release_mock = MagicMock()
        invalidate_mock = MagicMock()
        self.session_manager.acquire_session = acquire_mock
        self.session_manager.release_session = release_mock
        self.session_manager.invalidate_session = invalidate_mock
        self.acquire_session = acquire_mock
        self.release_session = release_mock
        self.invalidate_session = invalidate_mock

    def prepare_get_session(self, session_id):
        self.session_manager.get_session_id = MagicMock(return_value=session_id)

    def mock_request_lock(self, fence, first_call_err=None):
        self._mock_request("_request_lock", fence, first_call_err)

    def mock_request_unlock(self, result, first_call_err=None):
        self._mock_request("_request_unlock", result, first_call_err)

    def mock_request_try_lock(self, fence, first_call_err=None):
        self._mock_request("_request_try_lock", fence, first_call_err)

    def mock_request_get_lock_ownership_state(self, state, first_call_err=None):
        self._mock_request("_request_get_lock_ownership_state", state, first_call_err)

    def _mock_request(self, method_name, result, first_call_err):
        called = AtomicInteger()

        def mock(*_, **__):
            if called.get_and_increment() == 0 and first_call_err:
                return ImmediateExceptionFuture(first_call_err)
            return ImmediateFuture(result)

        setattr(self.proxy._wrapped, method_name, MagicMock(side_effect=mock))

    def assert_call_counts(self, acquire, release, invalidate):
        self.assertEqual(acquire, self.acquire_session.call_count)
        self.assertEqual(release, self.release_session.call_count)
        self.assertEqual(invalidate, self.invalidate_session.call_count)

    def prepare_state(self, fence, lock_count, session_id, t_id):
        return {
            "fence": fence,
            "lock_count": lock_count,
            "session_id": session_id,
            "thread_id": t_id,
        }

    def assert_no_lock_session_id(self):
        self.assertEqual(0, len(self.proxy._wrapped._lock_session_ids))

    def assert_lock_session_id(self, session_id):
        s_id = self.proxy._wrapped._lock_session_ids.get(thread_id(), None)
        self.assertEqual(session_id, s_id)
