import asyncio

from hazelcast.internal.asyncio_proxy.base import EntryEventType
from hazelcast.predicate import sql
from tests.integration.asyncio.base import SingleMemberTestCase
from tests.util import random_string, event_collector


class ReplicatedMapTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        return config

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.replicated_map = await self.client.get_replicated_map(random_string())

    async def asyncTearDown(self):
        await self.replicated_map.destroy()
        await super().asyncTearDown()

    async def test_add_entry_listener_item_added(self):
        collector = event_collector()
        await self.replicated_map.add_entry_listener(added_func=collector)
        await self.replicated_map.put("key", "value")

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(event, key="key", event_type=EntryEventType.ADDED, value="value")

        await self.assertTrueEventually(assert_event, 5)

    async def test_add_entry_listener_item_removed(self):
        collector = event_collector()
        await self.replicated_map.add_entry_listener(removed_func=collector)
        await self.replicated_map.put("key", "value")
        await self.replicated_map.remove("key")

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(
                event, key="key", event_type=EntryEventType.REMOVED, old_value="value"
            )

        await self.assertTrueEventually(assert_event, 5)

    async def test_add_entry_listener_item_updated(self):
        collector = event_collector()
        await self.replicated_map.add_entry_listener(updated_func=collector)
        await self.replicated_map.put("key", "value")
        await self.replicated_map.put("key", "new_value")

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(
                event,
                key="key",
                event_type=EntryEventType.UPDATED,
                old_value="value",
                value="new_value",
            )

        await self.assertTrueEventually(assert_event, 5)

    async def test_add_entry_listener_item_evicted(self):
        collector = event_collector()
        await self.replicated_map.add_entry_listener(evicted_func=collector)
        await self.replicated_map.put("key", "value", ttl=1)

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(
                event, key="key", event_type=EntryEventType.EVICTED, old_value="value"
            )

        await self.assertTrueEventually(assert_event, 10)

    async def test_add_entry_listener_with_key(self):
        collector = event_collector()
        await self.replicated_map.add_entry_listener(key="key1", added_func=collector)
        await self.replicated_map.put("key2", "value2")
        await self.replicated_map.put("key1", "value1")

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(
                event, key="key1", event_type=EntryEventType.ADDED, value="value1"
            )

        await self.assertTrueEventually(assert_event, 5)

    async def test_add_entry_listener_with_predicate(self):
        collector = event_collector()
        await self.replicated_map.add_entry_listener(
            predicate=sql("this == value1"), added_func=collector
        )
        await self.replicated_map.put("key2", "value2")
        await self.replicated_map.put("key1", "value1")

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(
                event, key="key1", event_type=EntryEventType.ADDED, value="value1"
            )

        await self.assertTrueEventually(assert_event, 5)

    async def test_add_entry_listener_with_key_and_predicate(self):
        collector = event_collector()
        await self.replicated_map.add_entry_listener(
            key="key1", predicate=sql("this == value3"), added_func=collector
        )
        await self.replicated_map.put("key2", "value2")
        await self.replicated_map.put("key1", "value1")
        await self.replicated_map.remove("key1")
        await self.replicated_map.put("key1", "value3")

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(
                event, key="key1", event_type=EntryEventType.ADDED, value="value3"
            )

        await self.assertTrueEventually(assert_event, 5)

    async def test_add_entry_listener_clear_all(self):
        collector = event_collector()
        await self.replicated_map.add_entry_listener(clear_all_func=collector)
        await self.replicated_map.put("key", "value")
        await self.replicated_map.clear()

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(
                event, event_type=EntryEventType.CLEAR_ALL, number_of_affected_entries=1
            )

        await self.assertTrueEventually(assert_event, 5)

    async def test_clear(self):
        await self.fill_map()

        await self.replicated_map.clear()

        self.assertEqual(0, await self.replicated_map.size())

    async def test_contains_key(self):
        await self.replicated_map.put("key", "value")

        self.assertTrue(await self.replicated_map.contains_key("key"))

    async def test_contains_key_when_missing(self):
        self.assertFalse(await self.replicated_map.contains_key("key"))

    async def test_contains_value(self):
        await self.replicated_map.put("key", "value")

        self.assertTrue(await self.replicated_map.contains_value("value"))

    async def test_contains_value_when_missing(self):
        self.assertFalse(await self.replicated_map.contains_value("value"))

    async def test_entry_set(self):
        map = await self.fill_map()

        async def assert_entry_set():
            self.assertCountEqual(map.items(), await self.replicated_map.entry_set())

        await self.assertTrueEventually(assert_entry_set)

    async def test_is_empty(self):
        await self.replicated_map.put("key", " value")

        self.assertFalse(await self.replicated_map.is_empty())

    async def test_is_empty_when_empty(self):
        self.assertTrue(await self.replicated_map.is_empty())

    async def test_key_set(self):
        map = await self.fill_map()

        async def assert_key_set():
            self.assertCountEqual(list(map.keys()), await self.replicated_map.key_set())

        await self.assertTrueEventually(assert_key_set)

    async def test_put_get(self):
        self.assertIsNone(await self.replicated_map.put("key", "value"))
        self.assertEqual("value", await self.replicated_map.put("key", "new_value"))

        self.assertEqual("new_value", await self.replicated_map.get("key"))

    async def test_put_all(self):
        map = {"key-%d" % x: "value-%d" % x for x in range(0, 10)}

        await self.replicated_map.put_all(map)

        async def assert_entry_set():
            self.assertCountEqual(map.items(), await self.replicated_map.entry_set())

        await self.assertTrueEventually(assert_entry_set)

    async def test_remove(self):
        await self.replicated_map.put("key", "value")
        self.assertEqual("value", await self.replicated_map.remove("key"))
        self.assertFalse(await self.replicated_map.contains_key("key"))

    async def test_remove_entry_listener(self):
        collector = event_collector()
        id = await self.replicated_map.add_entry_listener(added_func=collector)

        await self.replicated_map.put("key", "value")
        await self.assertTrueEventually(lambda: self.assertEqual(len(collector.events), 1))
        await self.replicated_map.remove_entry_listener(id)
        await self.replicated_map.put("key2", "value")

        await asyncio.sleep(1)
        self.assertEqual(len(collector.events), 1)

    async def test_size(self):
        map = await self.fill_map()
        self.assertEqual(len(map), await self.replicated_map.size())

    async def test_values(self):
        map = await self.fill_map()

        async def assert_values():
            self.assertCountEqual(list(map.values()), list(await self.replicated_map.values()))

        await self.assertTrueEventually(assert_values)

    def test_str(self):
        self.assertTrue(str(self.replicated_map).startswith("ReplicatedMap"))

    async def fill_map(self, count=10):
        map = {"key-%d" % x: "value-%d" % x for x in range(0, count)}
        for k, v in map.items():
            await self.replicated_map.put(k, v)
        return map
