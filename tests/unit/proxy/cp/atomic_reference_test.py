import unittest

from mock import MagicMock

from hazelcast.proxy.cp.atomic_reference import AtomicReference


class AtomicReferenceInvalidInputTest(unittest.TestCase):
    def setUp(self):
        self.atomic_ref = AtomicReference(MagicMock(), None, None, None, None)

    def test_alter(self):
        self._check_error("alter", None)

    def test_alter_and_get(self):
        self._check_error("alter_and_get", None)

    def test_get_and_alter(self):
        self._check_error("get_and_alter", None)

    def test_apply(self):
        self._check_error("apply", None)

    def _check_error(self, method_name, *args):
        fn = getattr(self.atomic_ref, method_name)
        self.assertTrue(callable(fn))
        with self.assertRaises(AssertionError):
            fn(*args)
