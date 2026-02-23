import asyncio

from hazelcast.internal.asyncio_proxy.base import ItemEventType
from tests.integration.asyncio.base import SingleMemberTestCase
from tests.util import random_string, event_collector


class ListTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        return config

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.list = await self.client.get_list(random_string())

    async def asyncTearDown(self):
        await self.list.destroy()
        await super().asyncTearDown()

    async def fill_list(self, items=None):
        if items is None:
            items = ["item-%d" % i for i in range(5)]
        for item in items:
            await self.list.add(item)
        return items

    async def test_add(self):
        result = await self.list.add("item")
        self.assertTrue(result)
        self.assertEqual(await self.list.get(0), "item")

    async def test_add_null_element(self):
        with self.assertRaises(AssertionError):
            await self.list.add(None)

    async def test_add_appends_to_end(self):
        await self.list.add("first")
        await self.list.add("second")
        self.assertEqual(await self.list.get(1), "second")

    async def test_add_at(self):
        await self.list.add("a")
        await self.list.add("c")
        await self.list.add_at(1, "b")
        self.assertEqual(await self.list.get(1), "b")
        self.assertEqual(await self.list.get(2), "c")

    async def test_add_at_null_element(self):
        with self.assertRaises(AssertionError):
            await self.list.add_at(0, None)

    async def test_add_all(self):
        items = ["1", "2", "3"]
        result = await self.list.add_all(items)
        self.assertTrue(result)
        self.assertEqual(await self.list.get(0), "1")
        self.assertEqual(await self.list.get(1), "2")
        self.assertEqual(await self.list.get(2), "3")

    async def test_add_all_null_element(self):
        with self.assertRaises(AssertionError):
            await self.list.add_all(["1", None, "3"])

    async def test_add_all_null_items(self):
        with self.assertRaises(AssertionError):
            await self.list.add_all(None)

    async def test_add_all_at(self):
        await self.list.add("0")
        items = ["1", "2", "3"]
        result = await self.list.add_all_at(1, items)
        self.assertTrue(result)
        tail = await self.list.list_iterator(1)
        self.assertCountEqual(tail, items)

    async def test_add_all_at_null_element(self):
        with self.assertRaises(AssertionError):
            await self.list.add_all_at(0, ["1", None])

    async def test_add_all_at_null_items(self):
        with self.assertRaises(AssertionError):
            await self.list.add_all_at(0, None)

    async def test_add_listener_item_added(self):
        collector = event_collector()
        await self.list.add_listener(include_value=False, item_added_func=collector)
        await self.list.add("item-value")

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertIsNone(event.item)
            self.assertEqual(event.event_type, ItemEventType.ADDED)

        await self.assertTrueEventually(assert_event, 5)

    async def test_add_listener_item_added_include_value(self):
        collector = event_collector()
        await self.list.add_listener(include_value=True, item_added_func=collector)
        await self.list.add("item-value")

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEqual(event.item, "item-value")
            self.assertEqual(event.event_type, ItemEventType.ADDED)

        await self.assertTrueEventually(assert_event, 5)

    async def test_add_listener_item_removed(self):
        collector = event_collector()
        await self.list.add_listener(include_value=False, item_removed_func=collector)
        await self.list.add("item-value")
        await self.list.remove("item-value")

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertIsNone(event.item)
            self.assertEqual(event.event_type, ItemEventType.REMOVED)

        await self.assertTrueEventually(assert_event, 5)

    async def test_add_listener_item_removed_include_value(self):
        collector = event_collector()
        await self.list.add_listener(include_value=True, item_removed_func=collector)
        await self.list.add("item-value")
        await self.list.remove("item-value")

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEqual(event.item, "item-value")
            self.assertEqual(event.event_type, ItemEventType.REMOVED)

        await self.assertTrueEventually(assert_event, 5)

    async def test_remove_listener(self):
        collector = event_collector()
        reg_id = await self.list.add_listener(include_value=False, item_added_func=collector)
        await self.list.remove_listener(reg_id)
        await self.list.add("item-value")

        await asyncio.sleep(1)
        self.assertEqual(len(collector.events), 0)

    async def test_clear(self):
        await self.fill_list()
        await self.list.clear()
        self.assertEqual(await self.list.size(), 0)

    async def test_contains_when_present(self):
        await self.fill_list(["a", "b", "c"])
        self.assertTrue(await self.list.contains("b"))

    async def test_contains_when_missing(self):
        await self.fill_list(["a", "b", "c"])
        self.assertFalse(await self.list.contains("z"))

    async def test_contains_null_element(self):
        with self.assertRaises(AssertionError):
            await self.list.contains(None)

    async def test_contains_all_when_present(self):
        items = ["1", "2", "3"]
        await self.fill_list(items)
        self.assertTrue(await self.list.contains_all(items))

    async def test_contains_all_when_partial(self):
        await self.fill_list(["1", "2", "3"])
        self.assertFalse(await self.list.contains_all(["1", "2", "99"]))

    async def test_contains_all_null_items(self):
        with self.assertRaises(AssertionError):
            await self.list.contains_all(None)

    async def test_get(self):
        await self.list.add("hello")
        self.assertEqual(await self.list.get(0), "hello")

    async def test_get_all(self):
        items = ["1", "2", "3"]
        await self.fill_list(items)
        self.assertEqual(await self.list.get_all(), items)

    async def test_get_all_empty(self):
        self.assertEqual(await self.list.get_all(), [])

    async def test_iterator(self):
        items = ["1", "2", "3"]
        await self.fill_list(items)
        result = await self.list.iterator()
        self.assertEqual(result, items)

    async def test_index_of(self):
        await self.fill_list(["a", "b", "c"])
        self.assertEqual(await self.list.index_of("b"), 1)

    async def test_index_of_not_found(self):
        await self.fill_list(["a", "b", "c"])
        self.assertEqual(await self.list.index_of("z"), -1)

    async def test_index_of_null_element(self):
        with self.assertRaises(AssertionError):
            await self.list.index_of(None)

    async def test_is_empty_when_empty(self):
        self.assertTrue(await self.list.is_empty())

    async def test_is_empty_when_not_empty(self):
        await self.list.add("x")
        self.assertFalse(await self.list.is_empty())

    async def test_last_index_of(self):
        await self.fill_list(["1", "2", "2", "3"])
        self.assertEqual(await self.list.last_index_of("2"), 2)

    async def test_last_index_of_not_found(self):
        await self.fill_list(["1", "2", "3"])
        self.assertEqual(await self.list.last_index_of("z"), -1)

    async def test_last_index_of_null_element(self):
        with self.assertRaises(AssertionError):
            await self.list.last_index_of(None)

    async def test_list_iterator(self):
        items = ["1", "2", "3"]
        await self.fill_list(items)
        result = await self.list.list_iterator()
        self.assertEqual(result, items)

    async def test_list_iterator_with_index(self):
        await self.fill_list(["1", "2", "3"])
        result = await self.list.list_iterator(1)
        self.assertEqual(result, ["2", "3"])

    async def test_remove_existing(self):
        await self.list.add("item")
        result = await self.list.remove("item")
        self.assertTrue(result)
        self.assertEqual(await self.list.size(), 0)

    async def test_remove_non_existing(self):
        result = await self.list.remove("no-such-item")
        self.assertFalse(result)

    async def test_remove_null_element(self):
        with self.assertRaises(AssertionError):
            await self.list.remove(None)

    async def test_remove_at(self):
        await self.list.add("item")
        removed = await self.list.remove_at(0)
        self.assertEqual(removed, "item")
        self.assertEqual(await self.list.size(), 0)

    async def test_remove_at_returns_previous_element(self):
        await self.fill_list(["a", "b", "c"])
        removed = await self.list.remove_at(1)
        self.assertEqual(removed, "b")
        self.assertEqual(await self.list.size(), 2)

    async def test_remove_all(self):
        await self.fill_list(["1", "2", "3"])
        result = await self.list.remove_all(["2", "3"])
        self.assertTrue(result)
        self.assertEqual(await self.list.get_all(), ["1"])

    async def test_remove_all_null_items(self):
        with self.assertRaises(AssertionError):
            await self.list.remove_all(None)

    async def test_retain_all(self):
        await self.fill_list(["1", "2", "3"])
        result = await self.list.retain_all(["2", "3"])
        self.assertTrue(result)
        self.assertEqual(await self.list.get_all(), ["2", "3"])

    async def test_retain_all_null_items(self):
        with self.assertRaises(AssertionError):
            await self.list.retain_all(None)

    async def test_size(self):
        items = ["1", "2", "3"]
        await self.fill_list(items)
        self.assertEqual(await self.list.size(), len(items))

    async def test_size_empty(self):
        self.assertEqual(await self.list.size(), 0)

    async def test_set_at(self):
        await self.fill_list(["1", "2", "3"])
        previous = await self.list.set_at(1, "22")
        self.assertEqual(previous, "2")
        self.assertEqual(await self.list.get(1), "22")

    async def test_set_at_null_element(self):
        await self.list.add("item")
        with self.assertRaises(AssertionError):
            await self.list.set_at(0, None)

    async def test_sub_list(self):
        await self.fill_list(["1", "2", "3", "4"])
        result = await self.list.sub_list(1, 3)
        self.assertEqual(result, ["2", "3"])

    async def test_sub_list_full_range(self):
        items = ["a", "b", "c"]
        await self.fill_list(items)
        result = await self.list.sub_list(0, 3)
        self.assertEqual(result, items)

    def test_str(self):
        self.assertTrue(str(self.list).startswith("List"))
