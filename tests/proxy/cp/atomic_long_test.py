from hazelcast.errors import DistributedObjectDestroyedError
from tests.proxy.cp import CPTestCase


class AtomicLongTest(CPTestCase):
    def setUp(self):
        self.atomic_long = self.client.cp_subsystem.get_atomic_long("long").blocking()

    def tearDown(self):
        self.atomic_long.set(0)

    def test_atomic_long_in_another_group(self):
        another_long = self.client.cp_subsystem.get_atomic_long("long@mygroup").blocking()
        self.assertEqual(1, another_long.increment_and_get())
        # the following value has to be 0,
        # as `along` belongs to the default CP group
        self.assertEqual(0, self.atomic_long.get())

    def test_use_after_destroy(self):
        another_long = self.client.cp_subsystem.get_atomic_long("another-long").blocking()
        another_long.destroy()
        # the next destroy call should be ignored
        another_long.destroy()

        with self.assertRaises(DistributedObjectDestroyedError):
            another_long.get()

        another_long2 = self.client.cp_subsystem.get_atomic_long("another-long").blocking()
        with self.assertRaises(DistributedObjectDestroyedError):
            another_long2.get()

    def test_initial_value(self):
        self.assertEqual(0, self.atomic_long.get())

    def test_add_and_get(self):
        self.assertEqual(33, self.atomic_long.add_and_get(33))
        self.assertEqual(33, self.atomic_long.get())

    def test_compare_and_set_when_condition_is_met(self):
        self.assertTrue(self.atomic_long.compare_and_set(0, 23))
        self.assertEqual(23, self.atomic_long.get())

    def test_compare_and_set_when_condition_is_not_met(self):
        self.assertFalse(self.atomic_long.compare_and_set(1, 23))
        self.assertEqual(0, self.atomic_long.get())

    def test_decrement_and_get(self):
        self.assertEqual(-1, self.atomic_long.decrement_and_get())
        self.assertEqual(-2, self.atomic_long.decrement_and_get())
        self.assertEqual(-2, self.atomic_long.get())

    def test_get_and_decrement(self):
        self.assertEqual(0, self.atomic_long.get_and_decrement())
        self.assertEqual(-1, self.atomic_long.get_and_decrement())
        self.assertEqual(-2, self.atomic_long.get())

    def test_get(self):
        self.assertEqual(0, self.atomic_long.get())
        self.atomic_long.set(11)
        self.assertEqual(11, self.atomic_long.get())
        long_max = 2**63 - 1
        self.atomic_long.set(long_max)
        self.assertEqual(long_max, self.atomic_long.get())
        long_min = -2**63
        self.atomic_long.set(long_min)
        self.assertEqual(long_min, self.atomic_long.get())

    def test_get_and_add(self):
        self.assertEqual(0, self.atomic_long.get_and_add(-100))
        self.assertEqual(-100, self.atomic_long.get())

    def test_get_and_set(self):
        self.assertEqual(0, self.atomic_long.get_and_set(123))
        self.assertEqual(123, self.atomic_long.get())

    def test_increment_and_get(self):
        self.assertEqual(1, self.atomic_long.increment_and_get())
        self.assertEqual(2, self.atomic_long.increment_and_get())
        self.assertEqual(2, self.atomic_long.get())

    def test_get_and_increment(self):
        self.assertEqual(0, self.atomic_long.get_and_increment())
        self.assertEqual(1, self.atomic_long.get_and_increment())
        self.assertEqual(2, self.atomic_long.get())

    def test_set(self):
        self.assertIsNone(self.atomic_long.set(42))
        self.assertEqual(42, self.atomic_long.get())
