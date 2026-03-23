from hazelcast.internal.asyncio_proxy.base import ItemEventType
from tests.integration.asyncio.base import SingleMemberTestCase
from tests.util import random_string, event_collector


class SetTest(SingleMemberTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.set = await self.client.get_set(random_string())

    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        return config

    async def asyncTearDown(self):
        await self.set.destroy()
        await super().asyncTearDown()

    async def test_add_entry_listener_item_added(self):
        collector = event_collector()
        await self.set.add_listener(include_value=False, item_added_func=collector)
        await self.set.add("item-value")

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEqual(event.item, None)
            self.assertEqual(event.event_type, ItemEventType.ADDED)

        await self.assertTrueEventually(assert_event, 5)

    async def test_add_entry_listener_item_added_include_value(self):
        collector = event_collector()
        await self.set.add_listener(include_value=True, item_added_func=collector)
        await self.set.add("item-value")

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEqual(event.item, "item-value")
            self.assertEqual(event.event_type, ItemEventType.ADDED)

        await self.assertTrueEventually(assert_event, 5)

    async def test_add_entry_listener_item_removed(self):
        collector = event_collector()
        await self.set.add_listener(include_value=False, item_removed_func=collector)
        await self.set.add("item-value")
        await self.set.remove("item-value")

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEqual(event.item, None)
            self.assertEqual(event.event_type, ItemEventType.REMOVED)

        await self.assertTrueEventually(assert_event, 5)

    async def test_add_entry_listener_item_removed_include_value(self):
        collector = event_collector()
        await self.set.add_listener(include_value=True, item_removed_func=collector)
        await self.set.add("item-value")
        await self.set.remove("item-value")

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEqual(event.item, "item-value")
            self.assertEqual(event.event_type, ItemEventType.REMOVED)

        await self.assertTrueEventually(assert_event, 5)

    async def test_remove_entry_listener_item_added(self):
        collector = event_collector()
        reg_id = await self.set.add_listener(include_value=False, item_added_func=collector)
        await self.set.remove_listener(reg_id)
        await self.set.add("item-value")

        def assert_event():
            self.assertEqual(len(collector.events), 0)
            if len(collector.events) > 0:
                event = collector.events[0]
                self.assertEqual(event.item, None)
                self.assertEqual(event.event_type, ItemEventType.ADDED)

        await self.assertTrueEventually(assert_event, 5)

    async def test_add(self):
        add_resp = await self.set.add("Test")
        result = await self.set.contains("Test")
        self.assertTrue(add_resp)
        self.assertTrue(result)

    async def test_add_null_element(self):
        with self.assertRaises(AssertionError):
            await self.set.add(None)

    async def test_add_all(self):
        _all = ["1", "2", "3"]
        add_resp = await self.set.add_all(_all)
        set_all = await self.set.get_all()
        self.assertCountEqual(_all, set_all)
        self.assertTrue(add_resp)

    async def test_add_all_null_element(self):
        _all = ["1", "2", "3", None]
        with self.assertRaises(AssertionError):
            await self.set.add_all(_all)

    async def test_add_all_null_elements(self):
        with self.assertRaises(AssertionError):
            await self.set.add_all(None)

    async def test_clear(self):
        _all = ["1", "2", "3"]
        await self.set.add_all(_all)
        size = await self.set.size()
        await self.set.clear()
        size_cleared = await self.set.size()
        self.assertEqual(size, len(_all))
        self.assertEqual(size_cleared, 0)

    async def test_contains(self):
        _all = ["1", "2", "3"]
        await self.set.add_all(_all)
        contains_result = await self.set.contains("2")
        self.assertTrue(contains_result)

    async def test_contains_all(self):
        _all = ["1", "2", "3"]
        await self.set.add_all(_all)
        contains_result = await self.set.contains_all(_all)
        self.assertTrue(contains_result)

    async def test_get_all(self):
        _all = ["1", "2", "3"]
        await self.set.add_all(_all)
        all_result = await self.set.get_all()
        self.assertCountEqual(all_result, _all)

    async def test_is_empty(self):
        is_empty = await self.set.is_empty()
        self.assertTrue(is_empty)

    async def test_remove(self):
        await self.set.add("Test")
        remove_result = await self.set.remove("Test")
        size = await self.set.size()
        self.assertTrue(remove_result)
        self.assertEqual(size, 0)

    async def test_remove_all(self):
        _all = ["1", "2", "3"]
        await self.set.add_all(_all)
        await self.set.remove_all(["2", "3"])
        result = await self.set.get_all()
        self.assertEqual(result, ["1"])

    async def test_retain_all(self):
        _all = ["1", "2", "3"]
        await self.set.add_all(_all)
        await self.set.retain_all(["2", "3"])
        result = await self.set.get_all()
        self.assertEqual(result, ["2", "3"])

    async def test_size(self):
        _all = ["1", "2", "3"]
        await self.set.add_all(_all)
        size = await self.set.size()
        self.assertEqual(size, len(_all))

    def test_str(self):
        self.assertTrue(str(self.set).startswith("Set"))
