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

    async def test_add_entry_listener_item_added(self):
        collector = event_collector()
        await self.list.add_listener(include_value=False, item_added_func=collector)
        await self.list.add("item-value")

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEqual(event.item, None)
            self.assertEqual(event.event_type, ItemEventType.ADDED)

        await self.assertTrueEventually(assert_event, 5)

    async def test_add_entry_listener_item_added_include_value(self):
        collector = event_collector()
        await self.list.add_listener(include_value=True, item_added_func=collector)
        await self.list.add("item-value")

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEqual(event.item, "item-value")
            self.assertEqual(event.event_type, ItemEventType.ADDED)

        await self.assertTrueEventually(assert_event, 5)

    async def test_add_entry_listener_item_removed(self):
        collector = event_collector()
        await self.list.add_listener(include_value=False, item_removed_func=collector)
        await self.list.add("item-value")
        await self.list.remove("item-value")

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEqual(event.item, None)
            self.assertEqual(event.event_type, ItemEventType.REMOVED)

        await self.assertTrueEventually(assert_event, 5)

    async def test_add_entry_listener_item_removed_include_value(self):
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

    async def test_remove_entry_listener_item_added(self):
        collector = event_collector()
        reg_id = await self.list.add_listener(include_value=False, item_added_func=collector)
        await self.list.remove_listener(reg_id)
        await self.list.add("item-value")

        def assert_event():
            self.assertEqual(len(collector.events), 0)
            if len(collector.events) > 0:
                event = collector.events[0]
                self.assertEqual(event.item, None)
                self.assertEqual(event.event_type, ItemEventType.ADDED)

        await self.assertTrueEventually(assert_event, 5)

    async def test_add(self):
        add_resp = await self.list.add("Test")
        result = await self.list.get(0)
        self.assertTrue(add_resp)
        self.assertEqual(result, "Test")

    async def test_add_null_element(self):
        with self.assertRaises(AssertionError):
            await self.list.add(None)

    async def test_add_at(self):
        await self.list.add_at(0, "Test0")
        await self.list.add_at(1, "Test1")
        result = await self.list.get(1)
        self.assertEqual(result, "Test1")

    async def test_add_at_null_element(self):
        with self.assertRaises(AssertionError):
            await self.list.add_at(0, None)

    async def test_add_all(self):
        _all = ["1", "2", "3"]
        add_resp = await self.list.add_all(_all)
        result0 = await self.list.get(0)
        result1 = await self.list.get(1)
        result2 = await self.list.get(2)
        self.assertTrue(add_resp)
        self.assertEqual(result0, "1")
        self.assertEqual(result1, "2")
        self.assertEqual(result2, "3")

    async def test_add_all_null_element(self):
        _all = ["1", "2", "3", None]
        with self.assertRaises(AssertionError):
            await self.list.add_all(_all)

    async def test_add_all_null_elements(self):
        with self.assertRaises(AssertionError):
            await self.list.add_all(None)

    async def test_add_all_at(self):
        await self.list.add_at(0, "0")
        _all = ["1", "2", "3"]
        add_resp = await self.list.add_all_at(1, _all)
        _all_resp = await self.list.list_iterator(1)
        self.assertTrue(add_resp)
        self.assertCountEqual(_all, _all_resp)

    async def test_add_all_at_null_element(self):
        _all = ["1", "2", "3", None]
        with self.assertRaises(AssertionError):
            await self.list.add_all_at(0, _all)

    async def test_add_all_at_null_elements(self):
        with self.assertRaises(AssertionError):
            await self.list.add_all_at(0, None)

    async def test_clear(self):
        _all = ["1", "2", "3"]
        await self.list.add_all(_all)
        size = await self.list.size()
        await self.list.clear()
        size_cleared = await self.list.size()
        self.assertEqual(size, len(_all))
        self.assertEqual(size_cleared, 0)

    async def test_contains(self):
        _all = ["1", "2", "3"]
        await self.list.add_all(_all)
        contains_result = await self.list.contains("2")
        self.assertTrue(contains_result)

    async def test_contains_all(self):
        _all = ["1", "2", "3"]
        await self.list.add_all(_all)
        contains_result = await self.list.contains_all(_all)
        self.assertTrue(contains_result)

    async def test_get_all(self):
        _all = ["1", "2", "3"]
        await self.list.add_all(_all)
        all_result = await self.list.get_all()
        self.assertEqual(all_result, _all)

    async def test_list_iterator(self):
        _all = ["1", "2", "3"]
        await self.list.add_all(_all)
        list_iter = await self.list.list_iterator(1)
        iter_result = []
        for item in list_iter:
            iter_result.append(item)
        self.assertEqual(iter_result, ["2", "3"])

    async def test_list_iterator2(self):
        _all = ["1", "2", "3"]
        await self.list.add_all(_all)
        list_iter = await self.list.list_iterator(1)
        iter_val = list_iter[1]
        self.assertEqual(iter_val, "3")

    async def test_iterator(self):
        _all = ["1", "2", "3"]
        await self.list.add_all(_all)
        list_iter = await self.list.iterator()
        iter_result = []
        for item in list_iter:
            iter_result.append(item)
        self.assertEqual(iter_result, _all)

    async def test_index_of(self):
        _all = ["1", "2", "3"]
        await self.list.add_all(_all)
        idx = await self.list.index_of("2")
        self.assertEqual(idx, 1)

    async def test_is_empty(self):
        is_empty = await self.list.is_empty()
        self.assertTrue(is_empty)

    async def test_last_index_of(self):
        _all = ["1", "2", "2", "3"]
        await self.list.add_all(_all)
        idx = await self.list.last_index_of("2")
        self.assertEqual(idx, 2)

    async def test_remove(self):
        await self.list.add("Test")
        remove_result = await self.list.remove("Test")
        size = await self.list.size()
        self.assertTrue(remove_result)
        self.assertEqual(size, 0)

    async def test_remove_at(self):
        await self.list.add("Test")
        remove_result = await self.list.remove_at(0)
        size = await self.list.size()
        self.assertTrue(remove_result)
        self.assertEqual(size, 0)

    async def test_remove_all(self):
        _all = ["1", "2", "3"]
        await self.list.add_all(_all)
        await self.list.remove_all(["2", "3"])
        result = await self.list.get_all()
        self.assertEqual(result, ["1"])

    async def test_retain_all(self):
        _all = ["1", "2", "3"]
        await self.list.add_all(_all)
        await self.list.retain_all(["2", "3"])
        result = await self.list.get_all()
        self.assertEqual(result, ["2", "3"])

    async def test_size(self):
        _all = ["1", "2", "3"]
        await self.list.add_all(_all)
        size = await self.list.size()
        self.assertEqual(size, len(_all))

    async def test_set_at(self):
        _all = ["1", "2", "3"]
        await self.list.add_all(_all)
        await self.list.set_at(1, "22")
        result = await self.list.get(1)
        self.assertEqual(result, "22")

    async def test_sub_list(self):
        _all = ["1", "2", "3"]
        await self.list.add_all(_all)
        sub_list = await self.list.sub_list(1, 3)
        self.assertEqual(sub_list, ["2", "3"])

    def test_str(self):
        self.assertTrue(str(self.list).startswith("List"))
