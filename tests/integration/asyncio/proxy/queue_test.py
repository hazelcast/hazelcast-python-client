import os

from hazelcast.errors import IllegalStateError
from hazelcast.internal.asyncio_proxy.base import ItemEventType
from tests.integration.asyncio.base import SingleMemberTestCase
from tests.util import random_string, event_collector


class QueueTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        return config

    @classmethod
    def configure_cluster(cls):
        path = os.path.abspath(__file__)
        dir_path = os.path.dirname(path)
        with open(os.path.join(dir_path, "../../backward_compatible/proxy/hazelcast.xml")) as f:
            return f.read()

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.queue = await self.client.get_queue("ClientQueueTest_" + random_string())

    async def asyncTearDown(self):
        await self.queue.destroy()
        await super().asyncTearDown()

    async def test_add_entry_listener_item_added(self):
        collector = event_collector()
        await self.queue.add_listener(include_value=False, item_added_func=collector)
        await self.queue.add("item-value")

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEqual(event.item, None)
            self.assertEqual(event.event_type, ItemEventType.ADDED)

        await self.assertTrueEventually(assert_event, 5)

    async def test_add_entry_listener_item_added_include_value(self):
        collector = event_collector()
        await self.queue.add_listener(include_value=True, item_added_func=collector)
        await self.queue.add("item-value")

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEqual(event.item, "item-value")
            self.assertEqual(event.event_type, ItemEventType.ADDED)

        await self.assertTrueEventually(assert_event, 5)

    async def test_add_entry_listener_item_removed(self):
        collector = event_collector()
        await self.queue.add_listener(include_value=False, item_removed_func=collector)
        await self.queue.add("item-value")
        await self.queue.remove("item-value")

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEqual(event.item, None)
            self.assertEqual(event.event_type, ItemEventType.REMOVED)

        await self.assertTrueEventually(assert_event, 5)

    async def test_add_entry_listener_item_removed_include_value(self):
        collector = event_collector()
        await self.queue.add_listener(include_value=True, item_removed_func=collector)
        await self.queue.add("item-value")
        await self.queue.remove("item-value")

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEqual(event.item, "item-value")
            self.assertEqual(event.event_type, ItemEventType.REMOVED)

        await self.assertTrueEventually(assert_event, 5)

    async def test_remove_entry_listener_item_added(self):
        collector = event_collector()
        reg_id = await self.queue.add_listener(include_value=False, item_added_func=collector)
        await self.queue.remove_listener(reg_id)
        await self.queue.add("item-value")

        def assert_event():
            self.assertEqual(len(collector.events), 0)

        await self.assertTrueEventually(assert_event, 5)

    async def test_add(self):
        add_resp = await self.queue.add("Test")
        result = await self.queue.contains("Test")
        self.assertTrue(add_resp)
        self.assertTrue(result)

    async def test_add_full(self):
        _all = ["1", "2", "3", "4", "5", "6"]
        await self.queue.add_all(_all)
        with self.assertRaises(IllegalStateError):
            await self.queue.add("cannot add this one")

    async def test_add_null_element(self):
        with self.assertRaises(AssertionError):
            await self.queue.add(None)

    async def test_add_all(self):
        _all = ["1", "2", "3"]
        add_resp = await self.queue.add_all(_all)
        q_all = await self.queue.iterator()
        self.assertCountEqual(_all, q_all)
        self.assertTrue(add_resp)

    async def test_add_all_null_element(self):
        _all = ["1", "2", "3", None]
        with self.assertRaises(AssertionError):
            await self.queue.add_all(_all)

    async def test_add_all_null_elements(self):
        with self.assertRaises(AssertionError):
            await self.queue.add_all(None)

    async def test_clear(self):
        _all = ["1", "2", "3"]
        await self.queue.add_all(_all)
        size = await self.queue.size()
        await self.queue.clear()
        size_cleared = await self.queue.size()
        self.assertEqual(size, len(_all))
        self.assertEqual(size_cleared, 0)

    async def test_contains(self):
        _all = ["1", "2", "3"]
        await self.queue.add_all(_all)
        contains_result = await self.queue.contains("2")
        self.assertTrue(contains_result)

    async def test_contains_all(self):
        _all = ["1", "2", "3"]
        await self.queue.add_all(_all)
        contains_result = await self.queue.contains_all(_all)
        self.assertTrue(contains_result)

    async def test_iterator(self):
        _all = ["1", "2", "3"]
        await self.queue.add_all(_all)
        all_result = await self.queue.iterator()
        self.assertCountEqual(all_result, _all)

    async def test_is_empty(self):
        is_empty = await self.queue.is_empty()
        self.assertTrue(is_empty)

    async def test_remaining_capacity(self):
        _all = ["1", "2", "3"]
        await self.queue.add_all(_all)
        capacity = await self.queue.remaining_capacity()
        self.assertEqual(capacity, 3)

    async def test_remove(self):
        await self.queue.add("Test")
        remove_result = await self.queue.remove("Test")
        size = await self.queue.size()
        self.assertTrue(remove_result)
        self.assertEqual(size, 0)

    async def test_remove_all(self):
        _all = ["1", "2", "3"]
        await self.queue.add_all(_all)
        await self.queue.remove_all(["2", "3"])
        result = await self.queue.iterator()
        self.assertEqual(result, ["1"])

    async def test_retain_all(self):
        _all = ["1", "2", "3"]
        await self.queue.add_all(_all)
        await self.queue.retain_all(["2", "3"])
        result = await self.queue.iterator()
        self.assertEqual(result, ["2", "3"])

    async def test_size(self):
        _all = ["1", "2", "3"]
        await self.queue.add_all(_all)
        size = await self.queue.size()
        self.assertEqual(size, len(_all))

    async def test_drain_to(self):
        _all = ["1", "2", "3"]
        await self.queue.add_all(_all)
        drain = []
        size = await self.queue.drain_to(drain)
        self.assertCountEqual(drain, _all)
        self.assertEqual(size, 3)

    async def test_peek(self):
        _all = ["1", "2", "3"]
        await self.queue.add_all(_all)
        peek_result = await self.queue.peek()
        self.assertEqual(peek_result, "1")

    async def test_put(self):
        await self.queue.put("Test")
        result = await self.queue.contains("Test")
        self.assertTrue(result)

    async def test_take(self):
        _all = ["1", "2", "3"]
        await self.queue.add_all(_all)
        take_result = await self.queue.take()
        self.assertEqual(take_result, "1")

    async def test_poll(self):
        _all = ["1", "2", "3"]
        await self.queue.add_all(_all)
        poll_result = await self.queue.poll()
        self.assertEqual(poll_result, "1")

    async def test_poll_timeout(self):
        _all = ["1", "2", "3"]
        await self.queue.add_all(_all)
        poll_result = await self.queue.poll(1)
        self.assertEqual(poll_result, "1")

    def test_str(self):
        self.assertTrue(str(self.queue).startswith("Queue"))
