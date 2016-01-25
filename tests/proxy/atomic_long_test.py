from threading import Event
from tests.base import SingleMemberTestCase
from tests.util import random_string


class AtomicLongTest(SingleMemberTestCase):
    def setUp(self):
        self.atomic_long = self.client.get_atomic_long(random_string()).blocking()
        self.reference = object()

    def tearDown(self):
        self.atomic_long.destroy()

    def test_add_and_get(self):
        self.assertEqual(2, self.atomic_long.add_and_get(2))
        self.assertEqual(4, self.atomic_long.add_and_get(2))

    def test_get_and_add(self):
        self.assertEqual(0, self.atomic_long.get_and_add(2))
        self.assertEqual(2, self.atomic_long.get_and_add(2))

    def test_compare_and_set_when_same(self):
        self.assertTrue(self.atomic_long.compare_and_set(0, 2))

    def test_compare_and_set_when_different(self):
        self.assertFalse(self.atomic_long.compare_and_set(2, 3))

    def test_decrement_and_get(self):
        self.assertEqual(-1, self.atomic_long.decrement_and_get())
        self.assertEqual(-2, self.atomic_long.decrement_and_get())

    def test_get_set(self):
        self.assertIsNone(self.atomic_long.set(100))
        self.assertEqual(100, self.atomic_long.get())

    def test_get_and_set(self):
        self.assertEqual(0, self.atomic_long.get_and_set(100))
        self.assertEqual(100, self.atomic_long.get_and_set(101))

    def test_increment_and_get(self):
        self.assertEqual(1, self.atomic_long.increment_and_get())
        self.assertEqual(2, self.atomic_long.increment_and_get())
        self.assertEqual(3, self.atomic_long.increment_and_get())

    def test_get_and_increment(self):
        self.assertEqual(0, self.atomic_long.get_and_increment())
        self.assertEqual(1, self.atomic_long.get_and_increment())
        self.assertEqual(2, self.atomic_long.get_and_increment())

    def test_str(self):
        self.assertTrue(str(self.atomic_long).startswith("AtomicLong"))
