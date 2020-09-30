import time
from threading import Thread

from hazelcast.errors import DistributedObjectDestroyedError, OperationTimeoutError
from hazelcast.future import ImmediateExceptionFuture
from hazelcast.util import AtomicInteger
from tests.proxy.cp import CPTestCase
from tests.util import random_string

inf = 2 ** 31 - 1


class CountDownLatchTest(CPTestCase):
    def test_latch_in_another_group(self):
        latch = self._get_latch()
        another_latch = self._get_latch()

        another_latch.try_set_count(42)
        self.assertEqual(42, another_latch.get_count())
        self.assertNotEqual(42, latch.get_count())

    def test_use_after_destroy(self):
        latch = self._get_latch()
        latch.destroy()
        # the next destroy call should be ignored
        latch.destroy()

        with self.assertRaises(DistributedObjectDestroyedError):
            latch.get_count()

        latch2 = self.client.cp_subsystem.get_count_down_latch(latch._proxy_name).blocking()

        with self.assertRaises(DistributedObjectDestroyedError):
            latch2.get_count()

    def test_await_latch_negative_timeout(self):
        latch = self._get_latch(1)
        self.assertFalse(latch.await_latch(-1))

    def test_await_latch_zero_timeout(self):
        latch = self._get_latch(1)
        self.assertFalse(latch.await_latch(0))

    def test_await_latch_with_timeout(self):
        latch = self._get_latch(1)
        start = time.time()
        self.assertFalse(latch.await_latch(0.1))
        time_passed = time.time() - start
        self.assertTrue(time_passed > 0.1)

    def test_await_latch_multiple_waiters(self):
        latch = self._get_latch(1)

        completed = AtomicInteger()

        def run():
            latch.await_latch(inf)
            completed.get_and_increment()

        count = 10
        threads = []
        for _ in range(count):
            t = Thread(target=run)
            threads.append(t)
            t.start()

        latch.count_down()

        def assertion():
            self.assertEqual(count, completed.get())

        self.assertTrueEventually(assertion)

        for i in range(count):
            threads[i].join()

    def test_await_latch_response_on_count_down(self):
        latch = self._get_latch()
        self.assertTrue(latch.await_latch(inf))
        self.assertTrue(latch.try_set_count(1))

        # make a non-blocking request
        future = latch._wrapped.await_latch(inf)
        t = Thread(target=lambda: latch.count_down())
        t.start()
        t.join()

        def assertion():
            self.assertTrue(future.done())
            self.assertTrue(future.result())

        self.assertTrueEventually(assertion)

    def test_count_down(self):
        latch = self._get_latch(10)

        for i in range(9, -1, -1):
            self.assertIsNone(latch.count_down())
            self.assertEqual(i, latch.get_count())

    def test_count_down_retry_on_timeout(self):
        latch = self._get_latch(1)

        original = latch._wrapped._request_count_down
        called_count = AtomicInteger()

        def mock(expected_round, invocation_uuid):
            if called_count.get_and_increment() < 2:
                return ImmediateExceptionFuture(OperationTimeoutError("xx"))
            return original(expected_round, invocation_uuid)

        latch._wrapped._request_count_down = mock

        latch.count_down()
        self.assertEqual(3, called_count.get())  # Will resolve on it's third call. First 2 throws timeout error
        self.assertEqual(0, latch.get_count())

    def test_get_count(self):
        latch = self._get_latch(1)
        self.assertEqual(1, latch.get_count())
        latch.count_down()
        self.assertEqual(0, latch.get_count())
        latch.try_set_count(10)
        self.assertEqual(10, latch.get_count())

    def test_try_set_count_with_negative_count(self):
        latch = self._get_latch()

        with self.assertRaises(AssertionError):
            latch.try_set_count(-1)

    def test_try_set_count_with_zero(self):
        latch = self._get_latch()

        with self.assertRaises(AssertionError):
            latch.try_set_count(0)

    def test_try_set_count(self):
        latch = self._get_latch()
        self.assertTrue(latch.try_set_count(3))
        self.assertEqual(3, latch.get_count())

    def test_try_set_count_when_count_is_already_set(self):
        latch = self._get_latch(1)
        self.assertFalse(latch.try_set_count(10))
        self.assertFalse(latch.try_set_count(20))
        self.assertEqual(1, latch.get_count())

    def _get_latch(self, initial_count=None):
        latch = self.client.cp_subsystem.get_count_down_latch("latch@" + random_string()).blocking()
        if initial_count is not None:
            self.assertTrue(latch.try_set_count(initial_count))
        return latch
