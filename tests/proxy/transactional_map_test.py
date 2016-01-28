from hazelcast.serialization.predicate import SqlPredicate
from tests.base import SingleMemberTestCase
from tests.util import random_string


class TransactionalMapTest(SingleMemberTestCase):
    def setUp(self):
        self.map = self.client.get_map(random_string()).blocking()

    def test_put(self):
        with self.client.new_transaction() as tx:
            tx_map = tx.get_map(self.map.name)
            self.assertIsNone(tx_map.put("key", "value"))

        self.assertEqual(self.map.get("key"), "value")

    def test_put_when_present(self):
        self.map.put("key", "value")
        with self.client.new_transaction() as tx:
            tx_map = tx.get_map(self.map.name)
            self.assertEqual(tx_map.put("key", "new_value"), "value")

        self.assertEqual(self.map.get("key"), "new_value")

    def test_put_if_absent(self):
        with self.client.new_transaction() as tx:
            tx_map = tx.get_map(self.map.name)
            self.assertIsNone(tx_map.put_if_absent("key", "value"))

        self.assertEqual(self.map.get("key"), "value")

    def test_put_if_absent_when_present(self):
        self.map.put("key", "value")

        with self.client.new_transaction() as tx:
            tx_map = tx.get_map(self.map.name)
            self.assertEqual(tx_map.put_if_absent("key", "new_value"), "value")

        self.assertEqual(self.map.get("key"), "value")

    def test_get(self):
        self.map.put("key", "value")
        with self.client.new_transaction() as tx:
            tx_map = tx.get_map(self.map.name)
            self.assertEqual(tx_map.get("key"), "value")

    def test_get_for_update(self):
        self.map.put("key", "value")
        with self.client.new_transaction() as tx:
            tx_map = tx.get_map(self.map.name)
            self.assertEqual(tx_map.get_for_update("key"), "value")
            self.assertTrue(self.map.is_locked("key"))

        self.assertFalse(self.map.is_locked("key"))

    def test_contains_key(self):
        self.map.put("key", "value")
        with self.client.new_transaction() as tx:
            tx_map = tx.get_map(self.map.name)
            self.assertTrue(tx_map.contains_key("key"))

    def test_contains_key_when_missing(self):
        with self.client.new_transaction() as tx:
            tx_map = tx.get_map(self.map.name)
            self.assertFalse(tx_map.contains_key("key"))

    def test_size(self):
        self.map.put("key", "value")

        with self.client.new_transaction() as tx:
            tx_map = tx.get_map(self.map.name)
            self.assertTrue(tx_map.size(), 1)

    def test_is_empty(self):
        with self.client.new_transaction() as tx:
            tx_map = tx.get_map(self.map.name)
            self.assertTrue(tx_map.is_empty())

    def test_is_empty_when_not_empty(self):
        self.map.put("key", "value")

        with self.client.new_transaction() as tx:
            tx_map = tx.get_map(self.map.name)
            self.assertFalse(tx_map.is_empty())

    def test_set(self):
        self.map.put("key", "value")

        with self.client.new_transaction() as tx:
            tx_map = tx.get_map(self.map.name)
            self.assertIsNone(tx_map.set("key", "new_value"))

        self.assertEqual(self.map.get("key"), "new_value")

    def test_replace(self):
        self.map.put("key", "value")

        with self.client.new_transaction() as tx:
            tx_map = tx.get_map(self.map.name)
            self.assertEqual("value", tx_map.replace("key", "new_value"))

        self.assertEqual(self.map.get("key"), "new_value")

    def test_replace_when_missing(self):
        with self.client.new_transaction() as tx:
            tx_map = tx.get_map(self.map.name)
            self.assertIsNone(tx_map.replace("key", "new_value"))

        self.assertIsNone(self.map.get("key"))

    def test_replace_if_same_when_same(self):
        self.map.put("key", "value")

        with self.client.new_transaction() as tx:
            tx_map = tx.get_map(self.map.name)
            self.assertTrue(tx_map.replace_if_same("key", "value", "new_value"))

        self.assertEqual(self.map.get("key"), "new_value")

    def test_replace_if_same_when_different(self):
        self.map.put("key", "value")

        with self.client.new_transaction() as tx:
            tx_map = tx.get_map(self.map.name)
            self.assertFalse(tx_map.replace_if_same("key", "another_value", "new_value"))

        self.assertEqual(self.map.get("key"), "value")

    def test_remove(self):
        self.map.put("key", "value")

        with self.client.new_transaction() as tx:
            tx_map = tx.get_map(self.map.name)
            self.assertEqual("value", tx_map.remove("key"))

        self.assertFalse(self.map.contains_key("key"))

    def test_remove_when_missing(self):
        with self.client.new_transaction() as tx:
            tx_map = tx.get_map(self.map.name)
            self.assertIsNone(tx_map.remove("key"))

    def test_remove_if_same_when_same(self):
        self.map.put("key", "value")

        with self.client.new_transaction() as tx:
            tx_map = tx.get_map(self.map.name)
            self.assertTrue(tx_map.remove_if_same("key", "value"))

        self.assertFalse(self.map.contains_key("key"))

    def test_remove_if_same_when_different(self):
        self.map.put("key", "value")

        with self.client.new_transaction() as tx:
            tx_map = tx.get_map(self.map.name)
            self.assertFalse(tx_map.remove_if_same("key", "another_value"))

        self.assertEqual(self.map.get("key"), "value")

    def test_delete(self):
        self.map.put("key", "value")

        with self.client.new_transaction() as tx:
            tx_map = tx.get_map(self.map.name)
            self.assertIsNone(tx_map.delete("key"))

        self.assertFalse(self.map.contains_key("key"))

    def test_key_set(self):
        self.map.put("key-1", "value-1")
        self.map.put("key-2", "value-2")
        self.map.put("key-3", "value-3")

        with self.client.new_transaction() as tx:
            tx_map = tx.get_map(self.map.name)
            self.assertItemsEqual(tx_map.key_set(), ["key-1", "key-2", "key-3"])

    def test_key_set_with_predicate(self):
        self.map.put("key-1", "value-1")
        self.map.put("key-2", "value-2")
        self.map.put("key-3", "value-3")

        with self.client.new_transaction() as tx:
            tx_map = tx.get_map(self.map.name)
            self.assertItemsEqual(tx_map.key_set(predicate=SqlPredicate("this == value-1")), ["key-1"])

    def test_values(self):
        self.map.put("key-1", "value-1")
        self.map.put("key-2", "value-2")
        self.map.put("key-3", "value-3")

        with self.client.new_transaction() as tx:
            tx_map = tx.get_map(self.map.name)
            self.assertItemsEqual(tx_map.values(), ["value-1", "value-2", "value-3"])

    def test_values_with_predicate(self):
        self.map.put("key-1", "value-1")
        self.map.put("key-2", "value-2")
        self.map.put("key-3", "value-3")

        with self.client.new_transaction() as tx:
            tx_map = tx.get_map(self.map.name)
            self.assertItemsEqual(tx_map.values(predicate=SqlPredicate("this == value-1")), ["value-1"])

    def test_str(self):
        with self.client.new_transaction() as tx:
            tx_map = tx.get_map(self.map.name)
            self.assertTrue(str(tx_map).startswith("TransactionalMap"))
