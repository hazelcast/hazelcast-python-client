from hazelcast.proxy.base import ItemEventType
from tests.base import SingleMemberTestCase
from tests.util import random_string, event_collector


class QueueTestCase(SingleMemberTestCase):
    def setUp(self):
        self.queue = self.client.get_queue(random_string())

    def test_add_entry_listener_item_added(self):
        collector = event_collector()
        self.queue.add_listener(include_value=False, item_added=collector)
        self.queue.add('item-value')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEqual(event.item, None)
            self.assertEqual(event.event_type, ItemEventType.added)

        self.assertTrueEventually(assert_event, 5)

    def test_add_entry_listener_item_added_include_value(self):
        collector = event_collector()
        self.queue.add_listener(include_value=True, item_added=collector)
        self.queue.add('item-value')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEqual(event.item, 'item-value')
            self.assertEqual(event.event_type, ItemEventType.added)

        self.assertTrueEventually(assert_event, 5)

    def test_add_entry_listener_item_removed(self):
        collector = event_collector()
        self.queue.add_listener(include_value=False, item_removed=collector)
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
        self.queue.add_listener(include_value=True, item_removed=collector)
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
        reg_id = self.queue.add_listener(include_value=False, item_added=collector)
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
        add_resp = self.queue.add("Test").result()
        result = self.queue.contains("Test").result()
        self.assertTrue(add_resp)
        self.assertTrue(result)

    def test_add_null_element(self):
        with self.assertRaises(AssertionError):
            self.queue.add(None)

    def test_add_all(self):
        _all = ["1", "2", "3"]
        add_resp = self.queue.add_all(_all).result()
        q_all = self.queue.iterator().result()
        self.assertItemsEqual(_all, q_all)
        self.assertTrue(add_resp)

    def test_add_all_null_element(self):
        _all = ["1", "2", "3", None]
        with self.assertRaises(AssertionError):
            self.queue.add_all(_all)

    def test_add_all_null_elements(self):
        with self.assertRaises(AssertionError):
            self.queue.add_all(None)

    def test_contains(self):
        _all = ["1", "2", "3"]
        self.queue.add_all(_all).result()
        contains_result = self.queue.contains("2")
        self.assertTrue(contains_result)

    def test_contains_all(self):
        _all = ["1", "2", "3"]
        self.queue.add_all(_all).result()
        contains_result = self.queue.contains(_all)
        self.assertTrue(contains_result)

    def test_iterator(self):
        _all = ["1", "2", "3"]
        self.queue.add_all(_all).result()
        all_result = self.queue.iterator().result()
        self.assertItemsEqual(all_result, _all)

    def test_is_empty(self):
        is_empty = self.queue.is_empty().result()
        self.assertTrue(is_empty)

    def test_remove(self):
        self.queue.add("Test").result()
        remove_result = self.queue.remove("Test").result()
        size = self.queue.size().result()
        self.assertTrue(remove_result)
        self.assertEqual(size, 0)

    def test_remove_all(self):
        _all = ["1", "2", "3"]
        self.queue.add_all(_all).result()
        self.queue.remove_all(["2", "3"])
        result = self.queue.iterator().result()
        self.assertEqual(result, ["1"])

    def test_retain_all(self):
        _all = ["1", "2", "3"]
        self.queue.add_all(_all).result()
        self.queue.retain_all(["2", "3"])
        result = self.queue.iterator().result()
        self.assertEqual(result, ["2", "3"])

    def test_size(self):
        _all = ["1", "2", "3"]
        self.queue.add_all(_all).result()
        size = self.queue.size().result()
        self.assertEqual(size, len(_all))

    def test_drain_to(self):
        _all = ["1", "2", "3"]
        self.queue.add_all(_all).result()
        drain = []
        size = self.queue.drain_to(drain).result()
        self.assertItemsEqual(drain, _all)
        self.assertEqual(size, 3)
