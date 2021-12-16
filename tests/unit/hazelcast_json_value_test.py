import json
import unittest

from hazelcast.core import HazelcastJsonValue


class HazelcastJsonValueTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.json_str = '{"key": "value"}'
        cls.json_obj = {"key": "value"}

    def test_hazelcast_json_value_construction_with_none(self):
        with self.assertRaises(AssertionError):
            HazelcastJsonValue(None)

    def test_hazelcast_json_value_construction_with_string(self):
        json_value = HazelcastJsonValue(self.json_str)
        self.assertEqual(self.json_str, json_value.to_string())

    def test_hazelcast_json_value_construction_with_json_serializable_object(self):
        json_value = HazelcastJsonValue(self.json_obj)
        self.assertEqual(json.dumps(self.json_obj), json_value.to_string())

    def test_hazelcast_json_value_construction_with_non_json_serializable_object(self):
        class A:
            def __init__(self):
                self.b = "c"

        with self.assertRaises(TypeError):
            HazelcastJsonValue(A())

    def test_hazelcast_json_value_loads(self):
        json_value = HazelcastJsonValue(self.json_str)
        self.assertEqual(self.json_obj, json_value.loads())
