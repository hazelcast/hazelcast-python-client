import unittest

from hzrc.client import HzRemoteController

import hazelcast
from tests.base import SingleMemberTestCase
from tests.util import random_string


class ListTestCase(SingleMemberTestCase):
    def setUp(self):
        self.list = self.client.get_list(random_string())

    def test_add(self):
        add_resp = self.list.add("Test").result()
        result = self.list.get(0).result()
        self.assertTrue(add_resp)
        self.assertEqual(result, "Test")

    def test_add_null_element(self):
        with self.assertRaises(AssertionError):
            self.list.add(None)

    def test_add_at(self):
        self.list.add_at(0, "Test0").result()
        self.list.add_at(1, "Test1").result()
        result = self.list.get(1).result()
        self.assertEqual(result, "Test1")

    def test_add_at_null_element(self):
        with self.assertRaises(AssertionError):
            self.list.add_at(0, None)

    def test_add_all(self):
        _all = ["1", "2", "3"]
        add_resp = self.list.add_all(_all).result()
        result0 = self.list.get(0).result()
        result1 = self.list.get(1).result()
        result2 = self.list.get(2).result()
        self.assertTrue(add_resp)
        self.assertEqual(result0, "1")
        self.assertEqual(result1, "2")
        self.assertEqual(result2, "3")

    def test_add_all_null_element(self):
        _all = ["1", "2", "3", None]
        with self.assertRaises(AssertionError):
            self.list.add_all(_all)

    def test_add_all_null_elements(self):
        with self.assertRaises(AssertionError):
            self.list.add_all(None)

    def test_add_all_at(self):
        _all = ["1", "2", "3"]
        add_resp = self.list.add_all(_all).result()
        result0 = self.list.get(0).result()
        result1 = self.list.get(1).result()
        result2 = self.list.get(2).result()
        self.assertTrue(add_resp)
        self.assertEqual(result0, "1")
        self.assertEqual(result1, "2")
        self.assertEqual(result2, "3")

    def test_add_all_at_null_element(self):
        _all = ["1", "2", "3", None]
        with self.assertRaises(AssertionError):
            self.list.add_all_at(0,_all)

    def test_add_all_at_null_elements(self):
        with self.assertRaises(AssertionError):
            self.list.add_all_at(0, None)

if __name__ == '__main__':
    unittest.main()
