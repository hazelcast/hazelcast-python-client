from tests.base import SingleMemberTestCase
from tests.util import random_string, configure_logging


class TransactionalListTest(SingleMemberTestCase):
    def setUp(self):
        configure_logging()
        self.list = self.client.get_list(random_string()).blocking()

    def test_add(self):
        with self.client.new_transaction() as tx:
            tx_list = tx.get_list(self.list.name)
            tx_list.add("item")

        self.assertSequenceEqual(self.list.get_all(), ["item"])

    def test_remove(self):
        self.list.add("item")
        with self.client.new_transaction() as tx:
            tx_list = tx.get_list(self.list.name)
            tx_list.remove("item")

        self.assertSequenceEqual(self.list.get_all(), [])

    def test_size(self):
        self.list.add("item")
        with self.client.new_transaction() as tx:
            tx_list = tx.get_list(self.list.name)
            tx_list.add("item")
            self.assertEqual(2, tx_list.size())

