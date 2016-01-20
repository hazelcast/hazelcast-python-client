from hazelcast.proxy.list import ItemEventType
from tests.base import SingleMemberTestCase
from tests.util import random_string, event_collector


class ListTestCase(SingleMemberTestCase):
    def setUp(self):
        self.list = self.client.get_list(random_string())

    def test_add_entry_listener_item_added(self):
        collector = event_collector()
        self.list.add_listener(include_value=False, item_added=collector)
        self.list.add('item-value')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEqual(event.item, None)
            self.assertEqual(event.event_type, ItemEventType.added)

        self.assertTrueEventually(assert_event, 5)

    def test_add_entry_listener_item_added_include_value(self):
        collector = event_collector()
        self.list.add_listener(include_value=True, item_added=collector)
        self.list.add('item-value')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEqual(event.item, 'item-value')
            self.assertEqual(event.event_type, ItemEventType.added)

        self.assertTrueEventually(assert_event, 5)

    def test_add_entry_listener_item_removed(self):
        collector = event_collector()
        self.list.add_listener(include_value=False, item_removed=collector)
        self.list.add('item-value')
        self.list.remove('item-value')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEqual(event.item, None)
            self.assertEqual(event.event_type, ItemEventType.removed)

        self.assertTrueEventually(assert_event, 5)

    def test_add_entry_listener_item_removed_include_value(self):
        collector = event_collector()
        self.list.add_listener(include_value=True, item_removed=collector)
        self.list.add('item-value')
        self.list.remove('item-value')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEqual(event.item, 'item-value')
            self.assertEqual(event.event_type, ItemEventType.removed)

        self.assertTrueEventually(assert_event, 5)

    def test_remove_entry_listener_item_added(self):
        collector = event_collector()
        self.list.add_listener(include_value=False, item_added=collector)
        self.list.add('item-value')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEqual(event.item, None)
            self.assertEqual(event.event_type, ItemEventType.added)

        self.assertTrueEventually(assert_event, 5)

    def test_add(self):
        add_resp = self.list.add("Test").result()
        result = self.list.get(0).result()
        self.assertTrue(add_resp)
        self.assertEqual(result, "Test")

    def test_add_null_element(self):
        with self.assertRaises(AssertionError):
            self.list.add(None)

    def test_add_at(self):
        self.list.add_at(0, "Test0").result()
        self.list.add_at(1, "Test1").result()
        result = self.list.get(1).result()
        self.assertEqual(result, "Test1")

    def test_add_at_null_element(self):
        with self.assertRaises(AssertionError):
            self.list.add_at(0, None)

    def test_add_all(self):
        _all = ["1", "2", "3"]
        add_resp = self.list.add_all(_all).result()
        result0 = self.list.get(0).result()
        result1 = self.list.get(1).result()
        result2 = self.list.get(2).result()
        self.assertTrue(add_resp)
        self.assertEqual(result0, "1")
        self.assertEqual(result1, "2")
        self.assertEqual(result2, "3")

    def test_add_all_null_element(self):
        _all = ["1", "2", "3", None]
        with self.assertRaises(AssertionError):
            self.list.add_all(_all)

    def test_add_all_null_elements(self):
        with self.assertRaises(AssertionError):
            self.list.add_all(None)

    def test_add_all_at(self):
        _all = ["1", "2", "3"]
        add_resp = self.list.add_all(_all).result()
        result0 = self.list.get(0).result()
        result1 = self.list.get(1).result()
        result2 = self.list.get(2).result()
        self.assertTrue(add_resp)
        self.assertEqual(result0, "1")
        self.assertEqual(result1, "2")
        self.assertEqual(result2, "3")

    def test_add_all_at_null_element(self):
        _all = ["1", "2", "3", None]
        with self.assertRaises(AssertionError):
            self.list.add_all_at(0, _all)

    def test_add_all_at_null_elements(self):
        with self.assertRaises(AssertionError):
            self.list.add_all_at(0, None)

    def test_clear(self):
        _all = ["1", "2", "3"]
        self.list.add_all(_all).result()
        size = self.list.size().result()
        self.list.clear()
        size_cleared = self.list.size().result()
        self.assertEqual(size, len(_all))
        self.assertEqual(size_cleared, 0)

    def test_contains(self):
        _all = ["1", "2", "3"]
        self.list.add_all(_all).result()
        contains_result = self.list.contains("2")
        self.assertTrue(contains_result)

    def test_contains_all(self):
        _all = ["1", "2", "3"]
        self.list.add_all(_all).result()
        contains_result = self.list.contains(_all)
        self.assertTrue(contains_result)

    def test_get_all(self):
        _all = ["1", "2", "3"]
        self.list.add_all(_all).result()
        contains_result = self.list.get_all().result()
        self.assertEqual(contains_result, _all)

    def test_index_of(self):
        _all = ["1", "2", "3"]
        self.list.add_all(_all).result()
        idx = self.list.index_of("2").result()
        self.assertEqual(idx, 1)

    def test_is_empty(self):
        is_empty = self.list.is_empty().result()
        self.assertTrue(is_empty)

    def test_last_index_of(self):
        _all = ["1", "2", "2", "3"]
        self.list.add_all(_all).result()
        idx = self.list.last_index_of("2").result()
        self.assertEqual(idx, 2)

    def test_remove(self):
        self.list.add("Test").result()
        remove_result = self.list.remove("Test").result()
        size = self.list.size().result()
        self.assertTrue(remove_result)
        self.assertEqual(size, 0)

    def test_remove_at(self):
        self.list.add("Test").result()
        remove_result = self.list.remove_at(0).result()
        size = self.list.size().result()
        self.assertTrue(remove_result)
        self.assertEqual(size, 0)

    def test_remove_all(self):
        _all = ["1", "2", "3"]
        self.list.add_all(_all).result()
        self.list.remove_all(["2", "3"])
        result = self.list.get_all().result()
        self.assertEqual(result, ["1"])

    def test_retain_all(self):
        _all = ["1", "2", "3"]
        self.list.add_all(_all).result()
        self.list.retain_all(["2", "3"])
        result = self.list.get_all().result()
        self.assertEqual(result, ["2", "3"])

    def test_size(self):
        _all = ["1", "2", "3"]
        self.list.add_all(_all).result()
        size = self.list.size().result()
        self.assertEqual(size, len(_all))

    def test_set_at(self):
        _all = ["1", "2", "3"]
        self.list.add_all(_all).result()
        self.list.set_at(1, "22").result()
        result = self.list.get(1).result()
        self.assertEqual(result, "22")

    def test_sub_list(self):
        _all = ["1", "2", "3"]
        self.list.add_all(_all).result()
        sub_list = self.list.sub_list(1, 3).result()
        self.assertEqual(sub_list, ["2", "3"])