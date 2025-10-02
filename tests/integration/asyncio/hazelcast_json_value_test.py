from hazelcast.core import HazelcastJsonValue
from hazelcast.predicate import greater, equal
from tests.integration.asyncio.base import SingleMemberTestCase


class HazelcastJsonValueWithMapTest(SingleMemberTestCase):
    @classmethod
    def setUpClass(cls):
        super(HazelcastJsonValueWithMapTest, cls).setUpClass()
        cls.json_str = '{"key": "value"}'
        cls.json_obj = {"key": "value"}

    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        return config

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.map = await self.client.get_map("json-test")

    async def asyncTearDown(self):
        await self.map.destroy()

    async def test_storing_hazelcast_json_value_as_key(self):
        json_value = HazelcastJsonValue(self.json_str)
        await self.map.put(json_value, 0)
        self.assertEqual(0, await self.map.get(json_value))

    async def test_storing_hazelcast_json_value_as_value(self):
        json_value = HazelcastJsonValue(self.json_str)
        await self.map.put(0, json_value)
        self.assertEqual(json_value.to_string(), (await self.map.get(0)).to_string())

    async def test_storing_hazelcast_json_value_with_invalid_str(self):
        json_value = HazelcastJsonValue('{"a')
        await self.map.put(0, json_value)
        self.assertEqual(json_value.to_string(), (await self.map.get(0)).to_string())

    async def test_querying_over_keys_with_hazelcast_json_value(self):
        json_value = HazelcastJsonValue({"a": 1})
        json_value2 = HazelcastJsonValue({"a": 3})
        await self.map.put(json_value, 1)
        await self.map.put(json_value2, 2)
        results = await self.map.key_set(greater("__key.a", 2))
        self.assertEqual(1, len(results))
        self.assertEqual(json_value2.to_string(), results[0].to_string())

    async def test_querying_nested_attr_over_keys_with_hazelcast_json_value(self):
        json_value = HazelcastJsonValue({"a": 1, "b": {"c": "d"}})
        json_value2 = HazelcastJsonValue({"a": 2, "b": {"c": "e"}})
        await self.map.put(json_value, 1)
        await self.map.put(json_value2, 2)
        results = await self.map.key_set(equal("__key.b.c", "d"))
        self.assertEqual(1, len(results))
        self.assertEqual(json_value.to_string(), results[0].to_string())

    async def test_querying_over_values_with_hazelcast_json_value(self):
        json_value = HazelcastJsonValue({"a": 1})
        json_value2 = HazelcastJsonValue({"a": 3})
        await self.map.put(1, json_value)
        await self.map.put(2, json_value2)
        results = await self.map.values(greater("a", 2))
        self.assertEqual(1, len(results))
        self.assertEqual(json_value2.to_string(), results[0].to_string())

    async def test_querying_nested_attr_over_values_with_hazelcast_json_value(self):
        json_value = HazelcastJsonValue({"a": 1, "b": {"c": "d"}})
        json_value2 = HazelcastJsonValue({"a": 2, "b": {"c": "e"}})
        await self.map.put(1, json_value)
        await self.map.put(2, json_value2)
        results = await self.map.values(equal("b.c", "d"))
        self.assertEqual(1, len(results))
        self.assertEqual(json_value.to_string(), results[0].to_string())
