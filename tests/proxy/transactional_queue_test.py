import os
from time import sleep
from unittest import skip

from tests.base import SingleMemberTestCase
from tests.util import random_string


class TransactionalQueueTest(SingleMemberTestCase):
    @classmethod
    def configure_cluster(cls):
        return open(os.path.join(os.path.dirname(__file__), "hazelcast_test.xml")).read()

    def setUp(self):
        self.queue = self.client.get_queue("ClientQueueTest_%s" % random_string()).blocking()

    def test_offer(self):
        with self.client.new_transaction() as tx:
            tx_queue = tx.get_queue(self.queue.name)
            self.assertTrue(tx_queue.offer("item"))

        self.assertEqual("item", self.queue.take())

    def test_offer_when_queue_full(self):
        self.queue.add_all(["item-%d" % x for x in xrange(0, 6)])

        with self.client.new_transaction() as tx:
            tx_queue = tx.get_queue(self.queue.name)
            self.assertFalse(tx_queue.offer("item"))

    def test_take(self):
        self.queue.offer("item")
        with self.client.new_transaction() as tx:
            tx_queue = tx.get_queue(self.queue.name)
            self.assertEqual("item", tx_queue.take())

        self.assertEqual(0, self.queue.size())

    def test_poll(self):
        self.queue.offer("item")
        with self.client.new_transaction() as tx:
            tx_queue = tx.get_queue(self.queue.name)
            self.assertEqual("item", tx_queue.poll())

        self.assertEqual(0, self.queue.size())

    def test_poll_when_missing(self):
        with self.client.new_transaction() as tx:
            tx_queue = tx.get_queue(self.queue.name)
            self.assertIsNone(tx_queue.poll())

    def test_poll_with_timeout(self):
        def offer():
            sleep(0.5)
            self.queue.offer("item")

        self.start_new_thread(offer)
        with self.client.new_transaction() as tx:
            tx_queue = tx.get_queue(self.queue.name)
            self.assertEqual("item", tx_queue.poll(5))

        self.assertEqual(0, self.queue.size())

    def test_peek(self):
        self.queue.offer("item")
        with self.client.new_transaction() as tx:
            tx_queue = tx.get_queue(self.queue.name)
            self.assertEqual("item", tx_queue.peek())

        self.assertEqual(1, self.queue.size())

    def test_peek_when_missing(self):
        with self.client.new_transaction() as tx:
            tx_queue = tx.get_queue(self.queue.name)
            self.assertIsNone(tx_queue.peek())

    def test_peek_with_timeout(self):
        def offer():
            sleep(0.5)
            self.queue.offer("item")

        self.start_new_thread(offer)
        with self.client.new_transaction() as tx:
            tx_queue = tx.get_queue(self.queue.name)
            self.assertEqual("item", tx_queue.peek(5))

        self.assertEqual(1, self.queue.size())

    def test_size(self):
        self.queue.offer("item-1")
        self.queue.offer("item-2")

        with self.client.new_transaction() as tx:
            tx_queue = tx.get_queue(self.queue.name)
            self.assertEqual(2, tx_queue.size())

    def test_str(self):
        with self.client.new_transaction() as tx:
            tx_queue = tx.get_queue(self.queue.name)
            self.assertTrue(str(tx_queue).startswith("TransactionalQueue"))
