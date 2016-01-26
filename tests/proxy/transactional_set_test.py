from tests.base import SingleMemberTestCase
from tests.util import random_string, configure_logging


class TransactionalSetTest(SingleMemberTestCase):
    def setUp(self):
        configure_logging()
        self.set = self.client.get_set(random_string()).blocking()

    def test_add(self):
        with self.client.new_transaction() as tx:
            tx_set = tx.get_set(self.set.name)
            tx_set.add("item")

        self.assertSequenceEqual(self.set.get_all(), ["item"])

    def test_remove(self):
        self.set.add("item")
        with self.client.new_transaction() as tx:
            tx_set = tx.get_set(self.set.name)
            tx_set.remove("item")

        self.assertSequenceEqual(self.set.get_all(), [])

    def test_size(self):
        self.set.add("item")
        with self.client.new_transaction() as tx:
            tx_set = tx.get_set(self.set.name)
            tx_set.add("item")
            self.assertEqual(2, tx_set.size())

    def test_str(self):
        with self.client.new_transaction() as tx:
            tx_set = tx.get_set(self.set.name)
            self.assertTrue(str(tx_set).startswith("TransactionalSet"))