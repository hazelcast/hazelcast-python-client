import asyncio
import itertools

from hazelcast.internal.asyncio_proxy.base import EntryEventType
from tests.integration.asyncio.base import SingleMemberTestCase
from tests.util import (
    random_string,
    event_collector,
    skip_if_client_version_older_than,
    skip_if_server_version_older_than,
)


class MultiMapTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        return config

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.multi_map = await self.client.get_multi_map(random_string())

    async def asyncTearDown(self):
        await self.multi_map.destroy()
        await super().asyncTearDown()

    async def test_add_entry_listener_item_added(self):
        collector = event_collector()
        await self.multi_map.add_entry_listener(include_value=True, added_func=collector)
        await self.multi_map.put("key", "value")

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(event, key="key", event_type=EntryEventType.ADDED, value="value")

        await self.assertTrueEventually(assert_event, 5)

    async def test_add_entry_listener_item_removed(self):
        collector = event_collector()
        await self.multi_map.add_entry_listener(include_value=True, removed_func=collector)
        await self.multi_map.put("key", "value")
        await self.multi_map.remove("key", "value")

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(
                event, key="key", event_type=EntryEventType.REMOVED, old_value="value"
            )

        await self.assertTrueEventually(assert_event, 5)

    async def test_add_entry_listener_clear_all(self):
        collector = event_collector()
        await self.multi_map.add_entry_listener(include_value=True, clear_all_func=collector)
        await self.multi_map.put("key", "value")
        await self.multi_map.clear()

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(
                event, event_type=EntryEventType.CLEAR_ALL, number_of_affected_entries=1
            )

        await self.assertTrueEventually(assert_event, 5)

    async def test_add_entry_listener_with_key(self):
        collector = event_collector()
        await self.multi_map.add_entry_listener(
            key="key1", include_value=True, added_func=collector
        )
        await self.multi_map.put("key2", "value2")
        await self.multi_map.put("key1", "value1")

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(
                event, key="key1", event_type=EntryEventType.ADDED, value="value1"
            )

        await self.assertTrueEventually(assert_event, 5)

    async def test_clear(self):
        await self.fill_map()
        await self.multi_map.clear()
        self.assertEqual(0, await self.multi_map.size())

    async def test_contains_key(self):
        await self.fill_map()
        self.assertTrue(await self.multi_map.contains_key("key-1"))
        self.assertFalse(await self.multi_map.contains_key("key-10"))

    async def test_contains_value(self):
        await self.fill_map()
        self.assertTrue(await self.multi_map.contains_value("value-1-1"))
        self.assertFalse(await self.multi_map.contains_value("value-10-10"))

    async def test_contains_entry(self):
        await self.fill_map()
        self.assertTrue(await self.multi_map.contains_entry("key-1", "value-1-1"))
        self.assertFalse(await self.multi_map.contains_entry("key-1", "value-1-10"))
        self.assertFalse(await self.multi_map.contains_entry("key-10", "value-1-1"))

    async def test_entry_set(self):
        mm = await self.fill_map()
        entry_list = []
        for key, values in mm.items():
            for value in values:
                entry_list.append((key, value))

        self.assertCountEqual(await self.multi_map.entry_set(), entry_list)

    async def test_key_set(self):
        keys = list((await self.fill_map()).keys())
        self.assertCountEqual(await self.multi_map.key_set(), keys)

    async def test_put_get(self):
        self.assertTrue(await self.multi_map.put("key", "value1"))
        self.assertTrue(await self.multi_map.put("key", "value2"))
        self.assertFalse(await self.multi_map.put("key", "value2"))
        self.assertCountEqual(await self.multi_map.get("key"), ["value1", "value2"])

    async def test_put_all(self):
        skip_if_server_version_older_than(self, self.client, "4.1")
        skip_if_client_version_older_than(self, "5.2")
        await self.multi_map.put_all(
            {"key1": ["value1", "value2", "value3"], "key2": ["value4", "value5", "value6"]}
        )
        self.assertCountEqual(await self.multi_map.get("key1"), ["value1", "value2", "value3"])
        self.assertCountEqual(await self.multi_map.get("key2"), ["value4", "value5", "value6"])

    async def test_remove(self):
        await self.multi_map.put("key", "value")
        self.assertTrue(await self.multi_map.remove("key", "value"))
        self.assertFalse(await self.multi_map.contains_key("key"))

    async def test_remove_when_missing(self):
        await self.multi_map.put("key", "value")
        self.assertFalse(await self.multi_map.remove("key", "other_value"))

    async def test_remove_all(self):
        await self.multi_map.put("key", "value")
        await self.multi_map.put("key", "value2")
        removed = await self.multi_map.remove_all("key")
        self.assertCountEqual(removed, ["value", "value2"])
        self.assertEqual(0, await self.multi_map.size())

    async def test_remove_entry_listener(self):
        collector = event_collector()
        id = await self.multi_map.add_entry_listener(added_func=collector)
        await self.multi_map.put("key", "value")
        await self.assertTrueEventually(lambda: self.assertEqual(len(collector.events), 1))
        await self.multi_map.remove_entry_listener(id)
        await self.multi_map.put("key2", "value")
        await asyncio.sleep(1)
        self.assertEqual(len(collector.events), 1)

    async def test_size(self):
        await self.fill_map(5)

        self.assertEqual(25, await self.multi_map.size())

    async def test_value_count(self):
        await self.fill_map(key_count=1, value_count=10)

        self.assertEqual(10, await self.multi_map.value_count("key-0"))

    async def test_values(self):
        values = list((await self.fill_map()).values())
        self.assertCountEqual(list(await self.multi_map.values()), itertools.chain(*values))

    def test_str(self):
        self.assertTrue(str(self.multi_map).startswith("MultiMap"))

    async def fill_map(self, key_count=5, value_count=5):
        map = {
            "key-%d" % x: ["value-%d-%d" % (x, y) for y in range(0, value_count)]
            for x in range(0, key_count)
        }
        for k, values in map.items():
            for v in values:
                await self.multi_map.put(k, v)
        return map
