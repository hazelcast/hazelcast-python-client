from tests.base import SingleMemberTestCase
from tests.util import random_string


class TransactionalMultiMapTest(SingleMemberTestCase):
    def setUp(self):
        self.multi_map = self.client.get_multi_map(random_string()).blocking()

    def test_put(self):
        with self.client.new_transaction() as tx:
            tx_multi_map = tx.get_multi_map(self.multi_map.name)
            self.assertTrue(tx_multi_map.put("key", "value-1"))
            self.assertTrue(tx_multi_map.put("key", "value-2"))

        self.assertItemsEqual(self.multi_map.get("key"), ["value-1", "value-2"])

    def test_put_when_present(self):
        self.multi_map.put("key", "value-1")
        with self.client.new_transaction() as tx:
            tx_multi_map = tx.get_multi_map(self.multi_map.name)
            self.assertFalse(tx_multi_map.put("key", "value-1"))

        self.assertItemsEqual(self.multi_map.get("key"), ["value-1"])

    def test_get(self):
        self.multi_map.put("key", "value-1")
        self.multi_map.put("key", "value-2")

        with self.client.new_transaction() as tx:
            tx_multi_map = tx.get_multi_map(self.multi_map.name)
            self.assertItemsEqual(tx_multi_map.get("key"), ["value-1", "value-2"])

    def test_get_when_missing(self):
        with self.client.new_transaction() as tx:
            tx_multi_map = tx.get_multi_map(self.multi_map.name)
            self.assertEqual(tx_multi_map.get("key"), [])

    def test_size(self):
        self.multi_map.put("key", "value-1")
        self.multi_map.put("key", "value-2")
        self.multi_map.put("key", "value-3")

        with self.client.new_transaction() as tx:
            tx_multi_map = tx.get_multi_map(self.multi_map.name)
            self.assertTrue(tx_multi_map.size(), 3)

    def test_remove_all(self):
        self.multi_map.put("key", "value-1")
        self.multi_map.put("key", "value-2")

        with self.client.new_transaction() as tx:
            tx_multi_map = tx.get_multi_map(self.multi_map.name)
            self.assertItemsEqual(["value-1", "value-2"], tx_multi_map.remove_all("key"))

        self.assertFalse(self.multi_map.contains_key("key"))

    def test_remove_all_when_missing(self):
        with self.client.new_transaction() as tx:
            tx_multi_map = tx.get_multi_map(self.multi_map.name)
            self.assertEqual([], tx_multi_map.remove_all("key"))

    def test_remove_when_same(self):
        self.multi_map.put("key", "value-1")
        self.multi_map.put("key", "value-2")

        with self.client.new_transaction() as tx:
            tx_multi_map = tx.get_multi_map(self.multi_map.name)
            self.assertTrue(tx_multi_map.remove("key", "value-1"))

        self.assertItemsEqual(["value-2"], self.multi_map.get("key"))

    def test_remove_when_different(self):
        self.multi_map.put("key", "value-1")
        self.multi_map.put("key", "value-2")

        with self.client.new_transaction() as tx:
            tx_multi_map = tx.get_multi_map(self.multi_map.name)
            self.assertFalse(tx_multi_map.remove("key", "value-3"))

        self.assertItemsEqual(["value-1", "value-2"], self.multi_map.get("key"))

    def test_value_count(self):
        self.multi_map.put("key", "value-1")
        self.multi_map.put("key", "value-2")
        self.multi_map.put("key", "value-3")

        with self.client.new_transaction() as tx:
            tx_multi_map = tx.get_multi_map(self.multi_map.name)
            self.assertEqual(tx_multi_map.value_count("key"), 3)

    def test_str(self):
        with self.client.new_transaction() as tx:
            tx_multi_map = tx.get_multi_map(self.multi_map.name)
            self.assertTrue(str(tx_multi_map).startswith("TransactionalMultiMap"))
