import os

from hazelcast.proxy.base import ItemEventType
from hazelcast.proxy.queue import Full
from tests.base import SingleMemberTestCase
from tests.util import random_string, event_collector


class QueueTest(SingleMemberTestCase):
    @classmethod
    def configure_cluster(cls):
        path = os.path.abspath(__file__)
        dir_path = os.path.dirname(path)
        return open(os.path.join(dir_path, "hazelcast_test.xml")).read()

    def setUp(self):
        self.queue = self.client.get_queue("ClientQueueTest_" + random_string()).blocking()

    def test_add_entry_listener_item_added(self):
        collector = event_collector()
        self.queue.add_listener(include_value=False, item_added_func=collector)
        self.queue.add('item-value')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEqual(event.item, None)
            self.assertEqual(event.event_type, ItemEventType.added)

        self.assertTrueEventually(assert_event, 5)

    def test_add_entry_listener_item_added_include_value(self):
        collector = event_collector()
        self.queue.add_listener(include_value=True, item_added_func=collector)
        self.queue.add('item-value')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEqual(event.item, 'item-value')
            self.assertEqual(event.event_type, ItemEventType.added)

        self.assertTrueEventually(assert_event, 5)

    def test_add_entry_listener_item_removed(self):
        collector = event_collector()
        self.queue.add_listener(include_value=False, item_removed_func=collector)
        self.queue.add('item-value')
        self.queue.remove('item-value')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEqual(event.item, None)
            self.assertEqual(event.event_type, ItemEventType.removed)

        self.assertTrueEventually(assert_event, 5)

    def test_add_entry_listener_item_removed_include_value(self):
        collector = event_collector()
        self.queue.add_listener(include_value=True, item_removed_func=collector)
        self.queue.add('item-value')
        self.queue.remove('item-value')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEqual(event.item, 'item-value')
            self.assertEqual(event.event_type, ItemEventType.removed)

        self.assertTrueEventually(assert_event, 5)

    def test_remove_entry_listener_item_added(self):
        collector = event_collector()
        reg_id = self.queue.add_listener(include_value=False, item_added_func=collector)
        self.queue.remove_listener(reg_id)
        self.queue.add('item-value')

        def assert_event():
            self.assertEqual(len(collector.events), 0)
            if len(collector.events) > 0:
                event = collector.events[0]
                self.assertEqual(event.item, None)
                self.assertEqual(event.event_type, ItemEventType.added)

        self.assertTrueEventually(assert_event, 5)

    def test_add(self):
        add_resp = self.queue.add("Test")
        result = self.queue.contains("Test")
        self.assertTrue(add_resp)
        self.assertTrue(result)

    def test_add_full(self):
        _all = ["1", "2", "3", "4", "5", "6"]
        self.queue.add_all(_all)
        with self.assertRaises(Full):
            self.queue.add("cannot add this one")

    def test_add_null_element(self):
        with self.assertRaises(AssertionError):
            self.queue.add(None)

    def test_add_all(self):
        _all = ["1", "2", "3"]
        add_resp = self.queue.add_all(_all)
        q_all = self.queue.iterator()
        self.assertItemsEqual(_all, q_all)
        self.assertTrue(add_resp)

    def test_add_all_null_element(self):
        _all = ["1", "2", "3", None]
        with self.assertRaises(AssertionError):
            self.queue.add_all(_all)

    def test_add_all_null_elements(self):
        with self.assertRaises(AssertionError):
            self.queue.add_all(None)

    def test_clear(self):
        _all = ["1", "2", "3"]
        self.queue.add_all(_all)
        size = self.queue.size()
        self.queue.clear()
        size_cleared = self.queue.size()
        self.assertEqual(size, len(_all))
        self.assertEqual(size_cleared, 0)

    def test_contains(self):
        _all = ["1", "2", "3"]
        self.queue.add_all(_all)
        contains_result = self.queue.contains("2")
        self.assertTrue(contains_result)

    def test_contains_all(self):
        _all = ["1", "2", "3"]
        self.queue.add_all(_all)
        contains_result = self.queue.contains_all(_all)
        self.assertTrue(contains_result)

    def test_iterator(self):
        _all = ["1", "2", "3"]
        self.queue.add_all(_all)
        all_result = self.queue.iterator()
        self.assertItemsEqual(all_result, _all)

    def test_is_empty(self):
        is_empty = self.queue.is_empty()
        self.assertTrue(is_empty)

    def test_remaining_capacity(self):
        _all = ["1", "2", "3"]
        self.queue.add_all(_all)
        capacity = self.queue.remaining_capacity()
        self.assertEqual(capacity, 3)

    def test_remove(self):
        self.queue.add("Test")
        remove_result = self.queue.remove("Test")
        size = self.queue.size()
        self.assertTrue(remove_result)
        self.assertEqual(size, 0)

    def test_remove_all(self):
        _all = ["1", "2", "3"]
        self.queue.add_all(_all)
        self.queue.remove_all(["2", "3"])
        result = self.queue.iterator()
        self.assertEqual(result, ["1"])

    def test_retain_all(self):
        _all = ["1", "2", "3"]
        self.queue.add_all(_all)
        self.queue.retain_all(["2", "3"])
        result = self.queue.iterator()
        self.assertEqual(result, ["2", "3"])

    def test_size(self):
        _all = ["1", "2", "3"]
        self.queue.add_all(_all)
        size = self.queue.size()
        self.assertEqual(size, len(_all))

    def test_drain_to(self):
        _all = ["1", "2", "3"]
        self.queue.add_all(_all)
        drain = []
        size = self.queue.drain_to(drain)
        self.assertItemsEqual(drain, _all)
        self.assertEqual(size, 3)

    def test_peek(self):
        _all = ["1", "2", "3"]
        self.queue.add_all(_all)
        peek_result = self.queue.peek()
        self.assertEqual(peek_result, "1")

    def test_put(self):
        self.queue.put("Test")
        result = self.queue.contains("Test")
        self.assertTrue(result)

    def test_take(self):
        _all = ["1", "2", "3"]
        self.queue.add_all(_all)
        take_result = self.queue.take()
        self.assertEqual(take_result, "1")

    def test_poll(self):
        _all = ["1", "2", "3"]
        self.queue.add_all(_all)
        poll_result = self.queue.poll()
        self.assertEqual(poll_result, "1")

    def test_poll_timeout(self):
        _all = ["1", "2", "3"]
        self.queue.add_all(_all)
        poll_result = self.queue.poll(1)
        self.assertEqual(poll_result, "1")

    def test_str(self):
        self.assertTrue(str(self.queue).startswith("Queue"))
