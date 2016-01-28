from hazelcast.exception import HazelcastSerializationError
from tests.base import SingleMemberTestCase
from tests.proxy.atomic_long_test import Function, FACTORY_ID
from tests.util import random_string


class AtomicReferenceTest(SingleMemberTestCase):
    def setUp(self):
        self.atomic_reference = self.client.get_atomic_reference(random_string()).blocking()

    def tearDown(self):
        self.atomic_reference.destroy()

    def test_alter(self):
        # TODO: Function must be defined on the server
        with self.assertRaises(HazelcastSerializationError):
            self.atomic_reference.alter(Function())

    def test_alter_and_get(self):
        # TODO: Function must be defined on the server
        with self.assertRaises(HazelcastSerializationError):
            self.atomic_reference.alter_and_get(Function())

    def test_apply(self):
        # TODO: Function must be defined on the server
        with self.assertRaises(HazelcastSerializationError):
            self.atomic_reference.apply(Function())

    def test_compare_and_set(self):
        self.assertTrue(self.atomic_reference.compare_and_set(None, "value"))
        self.assertTrue(self.atomic_reference.compare_and_set("value", "new_value"))

    def test_compare_and_set_when_different(self):
        self.assertFalse(self.atomic_reference.compare_and_set("value", "new_value"))

    def test_set_get(self):
        self.assertIsNone(self.atomic_reference.set("value"))
        self.assertEqual(self.atomic_reference.get(), "value")

    def test_get_and_set(self):
        self.assertIsNone(self.atomic_reference.get_and_set("value"))
        self.assertEqual("value", self.atomic_reference.get_and_set("new_value"))

    def test_set_and_get(self):
        self.assertEqual("value", self.atomic_reference.set_and_get("value"))
        self.assertEqual("new_value", self.atomic_reference.set_and_get("new_value"))

    def test_is_null_when_null(self):
        self.assertTrue(self.atomic_reference.is_null())

    def test_is_null_when_not_null(self):
        self.atomic_reference.set("value")
        self.assertFalse(self.atomic_reference.is_null())

    def test_clear(self):
        self.atomic_reference.set("value")
        self.atomic_reference.clear()

        self.assertIsNone(self.atomic_reference.get())

    def test_contains(self):
        self.atomic_reference.set("value")

        self.assertTrue(self.atomic_reference.contains("value"))

    def test_contains_when_missing(self):
        self.assertFalse(self.atomic_reference.contains("value"))

    def test_str(self):
        self.assertTrue(str(self.atomic_reference).startswith("AtomicReference"))
