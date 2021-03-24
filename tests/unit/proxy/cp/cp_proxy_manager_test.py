import unittest

from hazelcast.cp import _without_default_group_name, _get_object_name_for_proxy


class CPProxyManagerTest(unittest.TestCase):
    def test_without_default_group_name(self):
        self.assertEqual("test", _without_default_group_name("test@default"))
        self.assertEqual("test", _without_default_group_name("test@DEFAULT"))
        self.assertEqual("test@custom", _without_default_group_name("test@custom"))

    def test_without_default_group_name_with_multiple_group_names(self):
        with self.assertRaises(AssertionError):
            _without_default_group_name("test@default@@default")

    def test_without_default_group_name_with_metadata_group_name(self):
        with self.assertRaises(AssertionError):
            _without_default_group_name("test@METADATA")

        with self.assertRaises(AssertionError):
            _without_default_group_name("test@metadata")

    def test_get_object_name_for_proxy(self):
        self.assertEqual("test", _get_object_name_for_proxy("test@default"))
        self.assertEqual("test", _get_object_name_for_proxy("test@custom"))

    def test_get_object_name_for_proxy_empty_object_name(self):
        with self.assertRaises(AssertionError):
            _get_object_name_for_proxy("@default")

        with self.assertRaises(AssertionError):
            _get_object_name_for_proxy("  @default")

    def test_get_object_name_for_proxy_empty_proxy_name(self):
        with self.assertRaises(AssertionError):
            _get_object_name_for_proxy("test@")

        with self.assertRaises(AssertionError):
            _get_object_name_for_proxy("test@ ")
