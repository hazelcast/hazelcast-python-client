import json

from hazelcast.core import HazelcastJsonValue
from hazelcast.serialization.predicate import is_greater_than, is_equal_to
from tests.base import SingleMemberTestCase
from tests.util import set_attr
from unittest import TestCase


class HazelcastJsonValueTest(TestCase):
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
        class A(object):
            def __init__(self):
                self.b = 'c'

        with self.assertRaises(TypeError):
            HazelcastJsonValue(A())

    def test_hazelcast_json_value_loads(self):
        json_value = HazelcastJsonValue(self.json_str)
        self.assertEqual(self.json_obj, json_value.loads())


@set_attr(category=3.12)
class HazelcastJsonValueWithMapTest(SingleMemberTestCase):
    @classmethod
    def setUpClass(cls):
        super(HazelcastJsonValueWithMapTest, cls).setUpClass()
        cls.json_str = '{"key": "value"}'
        cls.json_obj = {"key": "value"}

    def setUp(self):
        self.map = self.client.get_map("json-test").blocking()

    def tearDown(self):
        self.map.destroy()

    def test_storing_hazelcast_json_value_as_key(self):
        json_value = HazelcastJsonValue(self.json_str)
        self.map.put(json_value, 0)
        self.assertEqual(0, self.map.get(json_value))

    def test_storing_hazelcast_json_value_as_value(self):
        json_value = HazelcastJsonValue(self.json_str)
        self.map.put(0, json_value)
        self.assertEqual(json_value.to_string(), self.map.get(0).to_string())

    def test_storing_hazelcast_json_value_with_invalid_str(self):
        json_value = HazelcastJsonValue('{"a')
        self.map.put(0, json_value)
        self.assertEqual(json_value.to_string(), self.map.get(0).to_string())

    def test_querying_over_keys_with_hazelcast_json_value(self):
        json_value = HazelcastJsonValue({"a": 1})
        json_value2 = HazelcastJsonValue({"a": 3})

        self.map.put(json_value, 1)
        self.map.put(json_value2, 2)

        results = self.map.key_set(is_greater_than("__key.a", 2))

        self.assertEqual(1, len(results))
        self.assertEqual(json_value2.to_string(), results[0].to_string())

    def test_querying_nested_attr_over_keys_with_hazelcast_json_value(self):
        json_value = HazelcastJsonValue({"a": 1, "b": {"c": "d"}})
        json_value2 = HazelcastJsonValue({"a": 2, "b": {"c": "e"}})

        self.map.put(json_value, 1)
        self.map.put(json_value2, 2)

        results = self.map.key_set(is_equal_to("__key.b.c", "d"))

        self.assertEqual(1, len(results))
        self.assertEqual(json_value.to_string(), results[0].to_string())

    def test_querying_over_values_with_hazelcast_json_value(self):
        json_value = HazelcastJsonValue({"a": 1})
        json_value2 = HazelcastJsonValue({"a": 3})

        self.map.put(1, json_value)
        self.map.put(2, json_value2)

        results = self.map.values(is_greater_than("a", 2))

        self.assertEqual(1, len(results))
        self.assertEqual(json_value2.to_string(), results[0].to_string())

    def test_querying_nested_attr_over_values_with_hazelcast_json_value(self):
        json_value = HazelcastJsonValue({"a": 1, "b": {"c": "d"}})
        json_value2 = HazelcastJsonValue({"a": 2, "b": {"c": "e"}})

        self.map.put(1, json_value)
        self.map.put(2, json_value2)

        results = self.map.values(is_equal_to("b.c", "d"))

        self.assertEqual(1, len(results))
        self.assertEqual(json_value.to_string(), results[0].to_string())
