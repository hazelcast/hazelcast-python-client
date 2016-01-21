from hazelcast.proxy.list import ItemEventType
from tests.base import SingleMemberTestCase
from tests.util import random_string, event_collector


class SetTestCase(SingleMemberTestCase):
    def setUp(self):
        self.set = self.client.get_set(random_string())

    def test_add_entry_listener_item_added(self):
        collector = event_collector()
        self.set.add_listener(include_value=False, item_added=collector)
        self.set.add('item-value')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEqual(event.item, None)
            self.assertEqual(event.event_type, ItemEventType.added)

        self.assertTrueEventually(assert_event, 5)

    def test_add_entry_listener_item_added_include_value(self):
        collector = event_collector()
        self.set.add_listener(include_value=True, item_added=collector)
        self.set.add('item-value')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEqual(event.item, 'item-value')
            self.assertEqual(event.event_type, ItemEventType.added)

        self.assertTrueEventually(assert_event, 5)

    def test_add_entry_listener_item_removed(self):
        collector = event_collector()
        self.set.add_listener(include_value=False, item_removed=collector)
        self.set.add('item-value')
        self.set.remove('item-value')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEqual(event.item, None)
            self.assertEqual(event.event_type, ItemEventType.removed)

        self.assertTrueEventually(assert_event, 5)

    def test_add_entry_listener_item_removed_include_value(self):
        collector = event_collector()
        self.set.add_listener(include_value=True, item_removed=collector)
        self.set.add('item-value')
        self.set.remove('item-value')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEqual(event.item, 'item-value')
            self.assertEqual(event.event_type, ItemEventType.removed)

        self.assertTrueEventually(assert_event, 5)

    def test_remove_entry_listener_item_added(self):
        collector = event_collector()
        reg_id = self.set.add_listener(include_value=False, item_added=collector)
        self.set.remove_listener(reg_id)
        self.set.add('item-value')

        def assert_event():
            self.assertEqual(len(collector.events), 0)
            if len(collector.events) > 0:
                event = collector.events[0]
                self.assertEqual(event.item, None)
                self.assertEqual(event.event_type, ItemEventType.added)

        self.assertTrueEventually(assert_event, 5)

    def test_add(self):
        add_resp = self.set.add("Test").result()
        result = self.set.contains("Test").result()
        self.assertTrue(add_resp)
        self.assertTrue(result)

    def test_add_null_element(self):
        with self.assertRaises(AssertionError):
            self.set.add(None)

    def test_add_all(self):
        _all = ["1", "2", "3"]
        add_resp = self.set.add_all(_all).result()
        set_all = self.set.get_all().result()
        self.assertItemsEqual(_all, set_all)
        self.assertTrue(add_resp)

    def test_add_all_null_element(self):
        _all = ["1", "2", "3", None]
        with self.assertRaises(AssertionError):
            self.set.add_all(_all)

    def test_add_all_null_elements(self):
        with self.assertRaises(AssertionError):
            self.set.add_all(None)

    def test_contains(self):
        _all = ["1", "2", "3"]
        self.set.add_all(_all).result()
        contains_result = self.set.contains("2")
        self.assertTrue(contains_result)

    def test_contains_all(self):
        _all = ["1", "2", "3"]
        self.set.add_all(_all).result()
        contains_result = self.set.contains(_all)
        self.assertTrue(contains_result)

    def test_get_all(self):
        _all = ["1", "2", "3"]
        self.set.add_all(_all).result()
        all_result = self.set.get_all().result()
        self.assertItemsEqual(all_result, _all)

    def test_is_empty(self):
        is_empty = self.set.is_empty().result()
        self.assertTrue(is_empty)

    def test_remove(self):
        self.set.add("Test").result()
        remove_result = self.set.remove("Test").result()
        size = self.set.size().result()
        self.assertTrue(remove_result)
        self.assertEqual(size, 0)

    def test_remove_all(self):
        _all = ["1", "2", "3"]
        self.set.add_all(_all).result()
        self.set.remove_all(["2", "3"])
        result = self.set.get_all().result()
        self.assertEqual(result, ["1"])

    def test_retain_all(self):
        _all = ["1", "2", "3"]
        self.set.add_all(_all).result()
        self.set.retain_all(["2", "3"])
        result = self.set.get_all().result()
        self.assertEqual(result, ["2", "3"])

    def test_size(self):
        _all = ["1", "2", "3"]
        self.set.add_all(_all).result()
        size = self.set.size().result()
        self.assertEqual(size, len(_all))
