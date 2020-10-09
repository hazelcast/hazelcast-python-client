import threading
import time
import unittest

from mock import MagicMock
from parameterized import parameterized

from hazelcast import HazelcastClient
from hazelcast.cp import SEMAPHORE_SERVICE
from hazelcast.errors import DistributedObjectDestroyedError, IllegalStateError, HazelcastRuntimeError, \
    SessionExpiredError, WaitKeyCancelledError
from hazelcast.future import ImmediateExceptionFuture, ImmediateFuture
from hazelcast.protocol import RaftGroupId
from hazelcast.proxy.cp.semaphore import SessionlessSemaphore, SessionAwareSemaphore
from hazelcast.util import AtomicInteger
from tests.proxy.cp import CPTestCase
from tests.util import random_string

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
        another_semaphore = self.client.cp_subsystem.get_semaphore(semaphore._proxy_name + "@another").blocking()

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

        semaphore2 = self.client.cp_subsystem.get_semaphore(semaphore._proxy_name).blocking()

        with self.assertRaises(DistributedObjectDestroyedError):
            semaphore2.init(1)

    def test_session_aware_semaphore_after_client_shutdown(self):
        semaphore = self.get_semaphore("sessionaware", 1)
        another_client = HazelcastClient(cluster_name=self.cluster.id)
        another_semaphore = another_client.cp_subsystem.get_semaphore(semaphore._proxy_name).blocking()
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
        start = time.time()
        f = semaphore._wrapped.acquire()
        event2.set()
        f.result()
        self.assertGreaterEqual(time.time() - start, 1)
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
        start = time.time()
        f = semaphore._wrapped.acquire()
        event2.set()

        with self.assertRaises(DistributedObjectDestroyedError):
            f.result()

        self.assertGreaterEqual(time.time() - start, 1)
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
        another_semaphore = another_client.cp_subsystem.get_semaphore(semaphore._proxy_name).blocking()
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
        start = time.time()
        self.assertFalse(semaphore.try_acquire(2, 1))
        self.assertGreaterEqual(time.time() - start, 1)
        self.assertEqual(1, semaphore.available_permits())

    def get_semaphore(self, semaphore_type, initialize_with=None):
        semaphore = self.client.cp_subsystem.get_semaphore(semaphore_type + random_string()).blocking()
        if initialize_with is not None:
            semaphore.init(initialize_with)
        self.semaphore = semaphore
        return semaphore


class SemaphoreIllegalArgumentTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.group_id = RaftGroupId("test", 0, 42)

    @parameterized.expand(SEMAPHORE_TYPES)
    def test_init_with_negative(self, semaphore_type):
        semaphore = self.get_semaphore(semaphore_type)

        with self.assertRaises(AssertionError):
            semaphore.init(-1)

    @parameterized.expand(SEMAPHORE_TYPES)
    def test_acquire_with_zero(self, semaphore_type):
        semaphore = self.get_semaphore(semaphore_type)

        with self.assertRaises(AssertionError):
            semaphore.acquire(0)

    @parameterized.expand(SEMAPHORE_TYPES)
    def test_acquire_with_negative(self, semaphore_type):
        semaphore = self.get_semaphore(semaphore_type)

        with self.assertRaises(AssertionError):
            semaphore.acquire(-100)

    @parameterized.expand(SEMAPHORE_TYPES)
    def test_reduce_permits_with_negative(self, semaphore_type):
        semaphore = self.get_semaphore(semaphore_type)

        with self.assertRaises(AssertionError):
            semaphore.reduce_permits(-11)

    @parameterized.expand(SEMAPHORE_TYPES)
    def test_increase_permits_with_negative(self, semaphore_type):
        semaphore = self.get_semaphore(semaphore_type)

        with self.assertRaises(AssertionError):
            semaphore.increase_permits(-11)

    @parameterized.expand(SEMAPHORE_TYPES)
    def test_release_with_zero(self, semaphore_type):
        semaphore = self.get_semaphore(semaphore_type)

        with self.assertRaises(AssertionError):
            semaphore.release(0)

    @parameterized.expand(SEMAPHORE_TYPES)
    def test_release_with_negative(self, semaphore_type):
        semaphore = self.get_semaphore(semaphore_type)

        with self.assertRaises(AssertionError):
            semaphore.release(-5)

    @parameterized.expand(SEMAPHORE_TYPES)
    def test_try_acquire_with_zero(self, semaphore_type):
        semaphore = self.get_semaphore(semaphore_type)

        with self.assertRaises(AssertionError):
            semaphore.try_acquire(0)

    @parameterized.expand(SEMAPHORE_TYPES)
    def test_try_acquire_with_negative(self, semaphore_type):
        semaphore = self.get_semaphore(semaphore_type)

        with self.assertRaises(AssertionError):
            semaphore.try_acquire(-112)

    def get_semaphore(self, semaphore_type):
        context = MagicMock()
        proxy_name = "semaphore@mygroup"
        object_name = "semaphore"
        if semaphore_type == "sessionless":
            return SessionlessSemaphore(context, self.group_id, SEMAPHORE_SERVICE, proxy_name, object_name)
        elif semaphore_type == "sessionaware":
            return SessionAwareSemaphore(context, self.group_id, SEMAPHORE_SERVICE, proxy_name, object_name)
        else:
            self.fail("Unknown semaphore type")


DRAIN_SESSION_ACQ_COUNT = 1024


class SessionAwareSemaphoreMockTest(unittest.TestCase):
    def setUp(self):
        self.acquire_session = MagicMock()
        self.release_session = MagicMock()
        self.invalidate_session = MagicMock()
        self.session_manager = MagicMock(acquire_session=self.acquire_session, release_session=self.release_session,
                                         invalidate_session=self.invalidate_session)
        context = MagicMock(proxy_session_manager=self.session_manager)
        self.group_id = RaftGroupId("test", 0, 42)
        self.semaphore = SessionAwareSemaphore(context, self.group_id, SEMAPHORE_SERVICE, "semaphore@mygroup",
                                               "semaphore").blocking()

    def test_acquire(self):
        # Everything works
        self.prepare_acquire_session(1)
        self.mock_request_acquire(True)
        self.assertIsNone(self.semaphore.acquire())
        self.assert_call_counts(1, 0, 0)
        self.assert_acquire_count(1)

    def test_acquire_when_acquire_session_fails(self):
        # First call to acquire session fails, should not retry
        self.prepare_acquire_session(-1, HazelcastRuntimeError())

        with self.assertRaises(HazelcastRuntimeError):
            self.semaphore.acquire()

        self.assert_call_counts(1, 0, 0)
        self.assert_acquire_count(1)

    def test_acquire_on_session_expired_error(self):
        # Session expired error comes from the server on acquire request,
        # retries and succeeds
        self.prepare_acquire_session(1)
        self.mock_request_acquire(True, SessionExpiredError())
        self.assertIsNone(self.semaphore.acquire())
        self.assert_call_counts(2, 0, 1)
        self.assert_acquire_count(1)

    def test_acquire_on_wait_key_cancelled_error(self):
        # Wait key cancelled error comes from the server, should not retry
        self.prepare_acquire_session(12)
        self.mock_request_acquire(True, WaitKeyCancelledError())

        with self.assertRaises(IllegalStateError):
            self.semaphore.acquire()

        self.assert_call_counts(1, 1, 0)
        self.assert_acquire_count(1)
        self.assert_release_count(12, 1)

    def test_acquire_on_unspecified_error(self):
        # Server sends another error, should not retry
        self.prepare_acquire_session(123)
        self.mock_request_acquire(False, HazelcastRuntimeError())

        with self.assertRaises(HazelcastRuntimeError):
            self.semaphore.acquire()

        self.assert_call_counts(1, 1, 0)
        self.assert_acquire_count(1)
        self.assert_release_count(123, 1)

    def test_drain(self):
        # Everything works
        self.prepare_acquire_session(42)
        self.mock_request_drain(10)
        self.assertEqual(10, self.semaphore.drain_permits())
        self.assert_call_counts(1, 1, 0)
        self.assert_acquire_count(DRAIN_SESSION_ACQ_COUNT)
        self.assert_release_count(42, DRAIN_SESSION_ACQ_COUNT - 10)

    def test_drain_when_acquire_session_fails(self):
        # First call to acquire session fails, should not retry
        self.prepare_acquire_session(-1, HazelcastRuntimeError())

        with self.assertRaises(HazelcastRuntimeError):
            self.semaphore.drain_permits()

        self.assert_call_counts(1, 0, 0)
        self.assert_acquire_count(DRAIN_SESSION_ACQ_COUNT)

    def test_drain_on_session_expired_error(self):
        # Session expired error comes from the server on drain request,
        # retries and succeeds
        self.prepare_acquire_session(99)
        self.mock_request_drain(101, SessionExpiredError())
        self.assertEqual(101, self.semaphore.drain_permits())
        self.assert_call_counts(2, 1, 1)
        self.assert_acquire_count(DRAIN_SESSION_ACQ_COUNT)
        self.assert_release_count(99, DRAIN_SESSION_ACQ_COUNT - 101)

    def test_drain_on_unspecified_error(self):
        # Server sends another error, should not retry
        self.prepare_acquire_session(123)
        self.mock_request_drain(False, HazelcastRuntimeError())

        with self.assertRaises(HazelcastRuntimeError):
            self.semaphore.drain_permits()

        self.assert_call_counts(1, 1, 0)
        self.assert_acquire_count(DRAIN_SESSION_ACQ_COUNT)
        self.assert_release_count(123, DRAIN_SESSION_ACQ_COUNT)

    def test_reduce_permits(self):
        # Everything works
        self.prepare_acquire_session(42)
        self.mock_request_change()
        self.assertIsNone(self.semaphore.reduce_permits(15))
        self.assert_call_counts(1, 1, 0)
        self.assert_acquire_count(1)
        self.assert_release_count(42, 1)

    def test_reduce_permits_when_acquire_session_fails(self):
        # First call to acquire session fails, should not retry
        self.prepare_acquire_session(-1, HazelcastRuntimeError())

        with self.assertRaises(HazelcastRuntimeError):
            self.semaphore.reduce_permits(12)

        self.assert_call_counts(1, 0, 0)
        self.assert_acquire_count(1)

    def test_reduce_permits_on_session_expired_error(self):
        # Session expired error comes from the server on change request
        self.prepare_acquire_session(99)
        self.mock_request_change(SessionExpiredError())

        with self.assertRaises(IllegalStateError):
            self.semaphore.reduce_permits(123)

        self.assert_call_counts(1, 1, 1)
        # Session will be invalidated before released, so release call is actually a no-op
        self.assert_acquire_count(1)
        self.assert_release_count(99, 1)

    def test_reduce_permits_on_unspecified_error(self):
        # Server sends another error
        self.prepare_acquire_session(1123)
        self.mock_request_change(HazelcastRuntimeError())

        with self.assertRaises(HazelcastRuntimeError):
            self.semaphore.reduce_permits(54)

        self.assert_call_counts(1, 1, 0)
        self.assert_acquire_count(1)
        self.assert_release_count(1123, 1)

    def test_increase_permits(self):
        # Everything works
        self.prepare_acquire_session(42)
        self.mock_request_change()
        self.assertIsNone(self.semaphore.increase_permits(15))
        self.assert_call_counts(1, 1, 0)
        self.assert_acquire_count(1)
        self.assert_release_count(42, 1)

    def test_increase_permits_when_acquire_session_fails(self):
        # First call to acquire session fails, should not retry
        self.prepare_acquire_session(-1, HazelcastRuntimeError())

        with self.assertRaises(HazelcastRuntimeError):
            self.semaphore.increase_permits(12)

        self.assert_call_counts(1, 0, 0)
        self.assert_acquire_count(1)

    def test_increase_permits_on_session_expired_error(self):
        # Session expired error comes from the server on change request
        self.prepare_acquire_session(99)
        self.mock_request_change(SessionExpiredError())

        with self.assertRaises(IllegalStateError):
            self.semaphore.increase_permits(123)

        self.assert_call_counts(1, 1, 1)
        # Session will be invalidated before released, so release call is actually a no-op
        self.assert_acquire_count(1)
        self.assert_release_count(99, 1)

    def test_increase_permits_on_unspecified_error(self):
        # Server sends another error
        self.prepare_acquire_session(1123)
        self.mock_request_change(HazelcastRuntimeError())

        with self.assertRaises(HazelcastRuntimeError):
            self.semaphore.increase_permits(54)

        self.assert_call_counts(1, 1, 0)
        self.assert_acquire_count(1)
        self.assert_release_count(1123, 1)

    def test_release(self):
        # Everything works
        self.prepare_get_session(42)
        self.mock_request_release()
        self.assertIsNone(self.semaphore.release(15))
        self.assert_call_counts(0, 1, 0)
        self.assert_release_count(42, 15)

    def test_release_no_session(self):
        # No session found for the release request on the client.
        self.prepare_get_session(-1)

        with self.assertRaises(IllegalStateError):
            self.semaphore.release()

        self.assert_call_counts(0, 0, 0)

    def test_release_on_session_expired_error(self):
        # Session expired error comes from the server on release request,
        self.prepare_get_session(99)
        self.mock_request_release(SessionExpiredError())

        with self.assertRaises(IllegalStateError):
            self.semaphore.release(123)

        self.assert_call_counts(0, 1, 1)
        # Session will be invalidated before released, so release call is actually a no-op
        self.assert_release_count(99, 123)

    def test_release_on_unspecified_error(self):
        # Server sends another error
        self.prepare_get_session(1123)
        self.mock_request_release(HazelcastRuntimeError())

        with self.assertRaises(HazelcastRuntimeError):
            self.semaphore.release(54)

        self.assert_call_counts(0, 1, 0)
        self.assert_release_count(1123, 54)

    def test_try_acquire(self):
        # Everything works
        self.prepare_acquire_session(1)
        self.mock_request_acquire(True)
        self.assertTrue(self.semaphore.try_acquire(15))
        self.assert_call_counts(1, 0, 0)
        self.assert_acquire_count(15)

    def test_try_acquire_when_acquire_session_fails(self):
        # First call to acquire session fails, should not retry
        self.prepare_acquire_session(-1, HazelcastRuntimeError())

        with self.assertRaises(HazelcastRuntimeError):
            self.semaphore.try_acquire()

        self.assert_call_counts(1, 0, 0)
        self.assert_acquire_count(1)

    def test_try_acquire_on_session_expired_error(self):
        # Session expired error comes from the server on acquire request,
        # determines the timeout
        self.prepare_acquire_session(1)
        self.mock_request_acquire(True, SessionExpiredError())
        self.assertFalse(self.semaphore.try_acquire())
        self.assert_call_counts(1, 0, 1)
        self.assert_acquire_count(1)

    def test_try_acquire_on_session_expired_error_when_not_timed_out(self):
        # Session expired error comes from the server on acquire request,
        # retries and succeeds
        self.prepare_acquire_session(123)
        self.mock_request_acquire(True, SessionExpiredError())
        self.assertTrue(self.semaphore.try_acquire(15, 3))
        self.assert_call_counts(2, 0, 1)
        self.assert_acquire_count(15)

    def test_try_acquire_on_wait_key_cancelled_error(self):
        # Wait key cancelled error comes from the server, should not retry
        self.prepare_acquire_session(12)
        self.mock_request_acquire(True, WaitKeyCancelledError())
        self.assertFalse(self.semaphore.try_acquire())
        self.assert_call_counts(1, 1, 0)
        self.assert_acquire_count(1)
        self.assert_release_count(12, 1)

    def test_try_acquire_on_unspecified_error(self):
        # Server sends another error, should not retry
        self.prepare_acquire_session(123)
        self.mock_request_acquire(False, HazelcastRuntimeError())

        with self.assertRaises(HazelcastRuntimeError):
            self.semaphore.try_acquire()

        self.assert_call_counts(1, 1, 0)
        self.assert_acquire_count(1)
        self.assert_release_count(123, 1)

    def assert_call_counts(self, acquire, release, invalidate):
        self.assertEqual(acquire, self.acquire_session.call_count)
        self.assertEqual(release, self.release_session.call_count)
        self.assertEqual(invalidate, self.invalidate_session.call_count)

    def assert_acquire_count(self, count):
        self.acquire_session.assert_called_with(self.group_id, count)

    def assert_release_count(self, session_id, count):
        self.release_session.assert_called_with(self.group_id, session_id, count)

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

    def mock_request_acquire(self, acquired, first_call_err=None):
        mock_request(self.semaphore, "_request_acquire", acquired, first_call_err)

    def mock_request_drain(self, count, first_call_err=None):
        mock_request(self.semaphore, "_request_drain", count, first_call_err)

    def mock_request_change(self, first_call_err=None):
        mock_request(self.semaphore, "_request_change", None, first_call_err)

    def mock_request_release(self, first_call_err=None):
        mock_request(self.semaphore, "_request_release", None, first_call_err)


class SessionlessSemaphoreProxy(unittest.TestCase):
    def setUp(self):
        self.session_manager = MagicMock()
        self.context = MagicMock(proxy_session_manager=self.session_manager)
        self.semaphore = SessionlessSemaphore(self.context, RaftGroupId("name", 0, 42), SEMAPHORE_SERVICE,
                                              "semaphore@mygroup", "semaphore").blocking()

    def test_acquire(self):
        # Everything works
        self.prepare_thread_id(12)
        self.mock_acquire_request(True)
        self.assertIsNone(self.semaphore.acquire())

    def test_acquire_when_get_thread_id_fails(self):
        # Client cannot even get the thread id
        self.prepare_thread_id(-1, HazelcastRuntimeError())

        with self.assertRaises(HazelcastRuntimeError):
            self.semaphore.acquire()

    def test_acquire_on_wait_key_cancelled_error(self):
        # Server sends wait key cancelled error, should not retry
        self.prepare_thread_id(23)
        self.mock_acquire_request(False, WaitKeyCancelledError())

        with self.assertRaises(IllegalStateError):
            self.semaphore.acquire()

    def test_try_acquire(self):
        # Everything works
        self.prepare_thread_id(12)
        self.mock_acquire_request(True)
        self.assertTrue(self.semaphore.try_acquire())

    def test_try_acquire_when_get_thread_id_fails(self):
        # Client cannot even get the thread id
        self.prepare_thread_id(-1, HazelcastRuntimeError())

        with self.assertRaises(HazelcastRuntimeError):
            self.semaphore.try_acquire()

    def test_try_acquire_on_wait_key_cancelled_error(self):
        # Server sends wait key cancelled error, should not retry
        self.prepare_thread_id(23)
        self.mock_acquire_request(False, WaitKeyCancelledError())

        with self.assertRaises(IllegalStateError):
            self.semaphore.try_acquire()

    def prepare_thread_id(self, thread_id, err=None):
        if err:
            value = ImmediateExceptionFuture(err)
        else:
            value = ImmediateFuture(thread_id)
        self.session_manager.get_or_create_unique_thread_id = MagicMock(return_value=value)

    def mock_acquire_request(self, acquired, first_call_err=None):
        mock_request(self.semaphore, "_request_acquire", acquired, first_call_err)


def mock_request(semaphore, method_name, result, first_call_err):
    called = AtomicInteger()

    def mock(*_, **__):
        if called.get_and_increment() == 0 and first_call_err:
            return ImmediateExceptionFuture(first_call_err)
        return ImmediateFuture(result)

    setattr(semaphore._wrapped, method_name, MagicMock(side_effect=mock))
