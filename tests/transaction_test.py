import unittest
from threading import Thread
import time

from hzrc.client import HzRemoteController

import hazelcast
import hazelcast.transaction
from hazelcast.exception import TransactionError
from tests.base import SingleMemberTestCase


class TransactionTest(SingleMemberTestCase):
    def test_begin_and_commit_transaction(self):
        transaction = self.client.new_transaction()
        transaction.begin()
        self.assertIsNotNone(transaction.transaction_id)
        self.assertEqual(transaction.state, hazelcast.transaction._STATE_ACTIVE)

        transaction.commit()
        self.assertEqual(transaction.state, hazelcast.transaction._STATE_COMMITTED)

    def test_begin_and_rollback_transaction(self):
        transaction = self.client.new_transaction()
        transaction.begin()
        self.assertIsNotNone(transaction.transaction_id)
        self.assertEqual(transaction.state, hazelcast.transaction._STATE_ACTIVE)

        transaction.rollback()
        self.assertEqual(transaction.state, hazelcast.transaction._STATE_ROLLED_BACK)

    def test_begin_transaction_twice(self):
        transaction = self.client.new_transaction()
        transaction.begin()
        with self.assertRaises(TransactionError):
            transaction.begin()
        transaction.rollback()

    def test_commit_inactive_transaction(self):
        transaction = self.client.new_transaction()
        with self.assertRaises(TransactionError):
            transaction.commit()

    def test_rollback_inactive_transaction(self):
        transaction = self.client.new_transaction()
        with self.assertRaises(TransactionError):
            transaction.rollback()

    def test_commit_transaction_twice(self):
        transaction = self.client.new_transaction()
        transaction.begin()
        transaction.commit()
        with self.assertRaises(TransactionError):
            transaction.commit()

    def test_rollback_transaction_twice(self):
        transaction = self.client.new_transaction()
        transaction.begin()
        transaction.rollback()
        with self.assertRaises(TransactionError):
            transaction.rollback()

    def test_commit_from_another_thread(self):
        transaction = self.client.new_transaction()
        t = Thread(target=transaction.begin)
        t.start()
        t.join()
        with self.assertRaises(TransactionError):
            transaction.commit()

    def test_rollback_from_another_thread(self):
        transaction = self.client.new_transaction()
        t = Thread(target=transaction.begin)
        t.start()
        t.join()
        with self.assertRaises(TransactionError):
            transaction.rollback()

    def test_operations_from_another_thread(self):
        transaction = self.client.new_transaction()
        ops = [transaction.get_map, transaction.get_list, transaction.get_multi_map, transaction.get_queue,
               transaction.get_set]

        t = Thread(target=transaction.begin)
        t.start()
        t.join()
        for op in ops:
            with self.assertRaises(TransactionError):
                op("name")

    def test_operations_before_transaction_started(self):
        transaction = self.client.new_transaction()
        ops = [transaction.get_map, transaction.get_list, transaction.get_multi_map, transaction.get_queue,
               transaction.get_set]

        for op in ops:
            with self.assertRaises(TransactionError):
                op("name")

    def test_nested_transactions_not_allowed(self):
        transaction = self.client.new_transaction()
        transaction.begin()

        nested_transaction = self.client.new_transaction()
        with self.assertRaises(TransactionError):
            nested_transaction.begin()

        transaction.rollback()

    def test_timeout(self):
        transaction = self.client.new_transaction(timeout=0.001)
        transaction.begin()
        time.sleep(0.001)
        with self.assertRaises(TransactionError):
            transaction.commit()
