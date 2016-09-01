import time

from hazelcast.proxy.base import EntryEventType
from hazelcast.serialization.predicate import SqlPredicate
from tests.base import SingleMemberTestCase
from tests.util import random_string, event_collector


class ReplicatedMapTest(SingleMemberTestCase):
    def setUp(self):
        self.replicated_map = self.client.get_replicated_map(random_string()).blocking()

    def tearDown(self):
        self.replicated_map.destroy()

    def test_add_entry_listener_item_added(self):
        collector = event_collector()
        self.replicated_map.add_entry_listener(added_func=collector)
        self.replicated_map.put('key', 'value')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(event, key='key', event_type=EntryEventType.added, value='value')

        self.assertTrueEventually(assert_event, 5)

    def test_add_entry_listener_item_removed(self):
        collector = event_collector()
        self.replicated_map.add_entry_listener(removed_func=collector)
        self.replicated_map.put('key', 'value')
        self.replicated_map.remove('key')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(event, key='key', event_type=EntryEventType.removed, old_value='value')

        self.assertTrueEventually(assert_event, 5)

    def test_add_entry_listener_item_updated(self):
        collector = event_collector()
        self.replicated_map.add_entry_listener(updated_func=collector)
        self.replicated_map.put('key', 'value')
        self.replicated_map.put('key', 'new_value')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(event, key='key', event_type=EntryEventType.updated, old_value='value',
                                  value='new_value')

        self.assertTrueEventually(assert_event, 5)

    def test_add_entry_listener_item_evicted(self):
        collector = event_collector()
        self.replicated_map.add_entry_listener(evicted_func=collector)
        self.replicated_map.put('key', 'value', ttl=1)

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(event, key='key', event_type=EntryEventType.evicted, old_value='value')

        self.assertTrueEventually(assert_event, 10)

    def test_add_entry_listener_with_key(self):
        collector = event_collector()
        id = self.replicated_map.add_entry_listener(key='key1', added_func=collector)
        self.replicated_map.put('key2', 'value2')
        self.replicated_map.put('key1', 'value1')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(event, key='key1', event_type=EntryEventType.added, value='value1')

        self.assertTrueEventually(assert_event, 5)

    def test_add_entry_listener_with_predicate(self):
        collector = event_collector()
        self.replicated_map.add_entry_listener(predicate=SqlPredicate("this == value1"), added_func=collector)
        self.replicated_map.put('key2', 'value2')
        self.replicated_map.put('key1', 'value1')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(event, key='key1', event_type=EntryEventType.added, value='value1')

        self.assertTrueEventually(assert_event, 5)

    def test_add_entry_listener_with_key_and_predicate(self):
        collector = event_collector()
        self.replicated_map.add_entry_listener(key='key1', predicate=SqlPredicate("this == value3"), added_func=collector)
        self.replicated_map.put('key2', 'value2')
        self.replicated_map.put('key1', 'value1')
        self.replicated_map.remove('key1')
        self.replicated_map.put('key1', 'value3')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(event, key='key1', event_type=EntryEventType.added, value='value3')

        self.assertTrueEventually(assert_event, 5)

    def test_add_entry_listener_clear_all(self):
        collector = event_collector()
        self.replicated_map.add_entry_listener(clear_all_func=collector)
        self.replicated_map.put('key', 'value')
        self.replicated_map.clear()

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(event, event_type=EntryEventType.clear_all, number_of_affected_entries=1)

        self.assertTrueEventually(assert_event, 5)

    def test_clear(self):
        self._fill_map()

        self.replicated_map.clear()

        self.assertEqual(0, self.replicated_map.size())

    def test_contains_key(self):
        self.replicated_map.put("key", "value")

        self.assertTrue(self.replicated_map.contains_key("key"))

    def test_contains_key_when_missing(self):
        self.assertFalse(self.replicated_map.contains_key("key"))

    def test_contains_value(self):
        self.replicated_map.put("key", "value")

        self.assertTrue(self.replicated_map.contains_value("value"))

    def test_contains_value_when_missing(self):
        self.assertFalse(self.replicated_map.contains_value("value"))

    def test_entry_set(self):
        map = self._fill_map()

        self.assertTrueEventually(
            lambda: self.assertItemsEqual(map.iteritems(), self.replicated_map.entry_set()))

    def test_is_empty(self):
        self.replicated_map.put("key", " value")

        self.assertFalse(self.replicated_map.is_empty())

    def test_is_empty_when_empty(self):
        self.assertTrue(self.replicated_map.is_empty())

    def test_key_set(self):
        map = self._fill_map()

        self.assertTrueEventually(lambda: self.assertItemsEqual(map.keys(), self.replicated_map.key_set()))

    def test_put_get(self):
        self.assertIsNone(self.replicated_map.put("key", "value"))
        self.assertEqual("value", self.replicated_map.put("key", "new_value"))

        self.assertEqual("new_value", self.replicated_map.get("key"))

    def test_put_all(self):
        map = {"key-%d" % x: "value-%d" % x for x in xrange(0, 10)}

        self.replicated_map.put_all(map)

        self.assertTrueEventually(lambda: self.assertItemsEqual(map.iteritems(), self.replicated_map.entry_set()))

    def test_remove(self):
        self.replicated_map.put("key", "value")
        self.assertEqual("value", self.replicated_map.remove("key"))
        self.assertFalse(self.replicated_map.contains_key("key"))

    def test_remove_entry_listener(self):
        collector = event_collector()
        id = self.replicated_map.add_entry_listener(added_func=collector)

        self.replicated_map.put('key', 'value')
        self.assertTrueEventually(lambda: self.assertEqual(len(collector.events), 1))
        self.replicated_map.remove_entry_listener(id)
        self.replicated_map.put('key2', 'value')

        time.sleep(1)
        self.assertEqual(len(collector.events), 1)

    def test_size(self):
        map = self._fill_map()
        self.assertEqual(len(map), self.replicated_map.size())

    def test_values(self):
        map = self._fill_map()

        self.assertTrueEventually(lambda: self.assertItemsEqual(map.values(), self.replicated_map.values()))

    def test_str(self):
        self.assertTrue(str(self.replicated_map).startswith("ReplicatedMap"))

    def _fill_map(self, count=10):
        map = {"key-%d" % x: "value-%d" % x for x in xrange(0, count)}
        for k, v in map.iteritems():
            self.replicated_map.put(k, v)
        return map
