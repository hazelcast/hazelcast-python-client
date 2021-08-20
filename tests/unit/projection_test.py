import unittest

from hazelcast.projection import single_attribute, multi_attribute


class ProjectionsInvalidInputTest(unittest.TestCase):
    def test_single_attribute_with_any_operator(self):
        with self.assertRaises(ValueError):
            single_attribute("foo[any]")

    def test_single_attribute_with_empty_path(self):
        with self.assertRaises(ValueError):
            single_attribute("")

    def test_multi_attribute_with_no_paths(self):
        with self.assertRaises(ValueError):
            multi_attribute()

    def test_multi_attribute_with_any_operator(self):
        with self.assertRaises(ValueError):
            multi_attribute("valid", "invalid[any]")

    def test_multi_attribute_with_empty_path(self):
        with self.assertRaises(ValueError):
            multi_attribute("valid", "")
