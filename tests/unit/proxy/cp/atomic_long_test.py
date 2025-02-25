import unittest

import pytest
from mock import MagicMock

from hazelcast.proxy.cp.atomic_long import AtomicLong


@pytest.mark.enterprise
class AtomicLongInvalidInputTest(unittest.TestCase):
    def setUp(self):
        self.atomic_long = AtomicLong(MagicMock(), None, None, None, None)

    def test_add_and_get(self):
        self._check_error("add_and_get", "1")

    def test_compare_and_set(self):
        invalid_inputs = [("a", 1), (1, "b"), ("c", "d")]
        for e, u in invalid_inputs:
            self._check_error("compare_and_set", e, u)

    def test_get_and_add(self):
        self._check_error("get_and_add", None)

    def test_get_and_set(self):
        self._check_error("get_and_set", 1.1)

    def test_set(self):
        self._check_error("set", "a")

    def test_alter(self):
        self._check_error("alter", None)

    def test_alter_and_get(self):
        self._check_error("alter_and_get", None)

    def test_get_and_alter(self):
        self._check_error("get_and_alter", None)

    def test_apply(self):
        self._check_error("apply", None)

    def _check_error(self, method_name, *args):
        fn = getattr(self.atomic_long, method_name)
        self.assertTrue(callable(fn))
        with self.assertRaises(AssertionError):
            fn(*args)
