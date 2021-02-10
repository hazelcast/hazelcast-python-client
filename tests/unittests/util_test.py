import unittest

from hazelcast.util import ensure_list, ensure_dict, ensure_value, ensure_type


class EnsureTypeTestCase(unittest.TestCase):
    def test_ensure_list_fail_not_list(self):
        with self.assertRaises(TypeError) as cm:
            ensure_list("foo", str)
        self.assertEqual("value must be a list", cm.exception.args[0])

    def test_ensure_list_fail_invalid_item_type(self):
        with self.assertRaises(TypeError) as cm:
            ensure_list(["foo", 2, "bar"], str)
        self.assertEqual("value items must be <class 'str'>", cm.exception.args[0])

    def test_ensure_list_success(self):
        ensure_list([1, 2, 3], int)

    def test_ensure_dict_fail_not_dict(self):
        with self.assertRaises(TypeError) as cm:
            ensure_dict("foo", int, str)
        self.assertEqual("value must be a dict", cm.exception.args[0])

    def test_ensure_dict_fail_item_key(self):
        with self.assertRaises(TypeError) as cm:
            ensure_dict({1: 2}, str, int)
        self.assertEqual("Keys of value must be str", cm.exception.args[0])

    def test_ensure_dict_fail_item_value(self):
        with self.assertRaises(TypeError) as cm:
            ensure_dict({"foo": "bar"}, str, int)
        self.assertEqual("Values of value must be int", cm.exception.args[0])

    def test_ensure_dict_success(self):
        ensure_dict({"foo": 1}, str, int)

    def test_ensure_type_tuple_success(self):
        ensure_type((1, 2), tuple)


class EnsureValueTestCase(unittest.TestCase):
    def test_ensure_value_fail_check(self):
        with self.assertRaises(ValueError) as cm:
            ensure_value(-10, check=lambda n: n >= 0, failure_msg="value must be non-negative")
        self.assertEqual("value must be non-negative", cm.exception.args[0])

    def test_ensure_value_success(self):
        ensure_value(10, check=lambda n: n >= 0, failure_msg="value must be non-negative")
