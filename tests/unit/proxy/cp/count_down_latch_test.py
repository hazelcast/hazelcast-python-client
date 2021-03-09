import unittest

from mock import MagicMock

from hazelcast.proxy.cp.count_down_latch import CountDownLatch


class CountDownLatchInvalidInputTest(unittest.TestCase):
    def setUp(self):
        self.latch = CountDownLatch(MagicMock(), None, None, None, None)

    def test_await_latch(self):
        self._check_error("await_latch", "a")

    def test_try_set_count(self):
        self._check_error("try_set_count", 1.1)

    def test_try_set_count_on_negative_input(self):
        self._check_error("try_set_count", -1)

    def test_try_set_count_on_zero(self):
        self._check_error("try_set_count", 0)

    def _check_error(self, method_name, *args):
        fn = getattr(self.latch, method_name)
        self.assertTrue(callable(fn))
        with self.assertRaises(AssertionError):
            fn(*args)
