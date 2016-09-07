import time
from unittest import skip

import itertools

from hazelcast.exception import HazelcastError
from hazelcast.proxy.map import EntryEventType
from tests.base import SingleMemberTestCase
from tests.util import random_string, event_collector


class MultiMapTest(SingleMemberTestCase):
    def setUp(self):
        self.multi_map = self.client.get_multi_map(random_string()).blocking()

    def tearDown(self):
        self.multi_map.destroy()

    def test_add_entry_listener_item_added(self):
        collector = event_collector()
        self.multi_map.add_entry_listener(include_value=True, added_func=collector)
        self.multi_map.put('key', 'value')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(event, key='key', event_type=EntryEventType.added, value='value')

        self.assertTrueEventually(assert_event, 5)

    def test_add_entry_listener_item_removed(self):
        collector = event_collector()
        self.multi_map.add_entry_listener(include_value=True, removed_func=collector)
        self.multi_map.put('key', 'value')
        self.multi_map.remove('key', 'value')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(event, key='key', event_type=EntryEventType.removed, old_value='value')

        self.assertTrueEventually(assert_event, 5)

    def test_add_entry_listener_clear_all(self):
        collector = event_collector()
        self.multi_map.add_entry_listener(include_value=True, clear_all_func=collector)
        self.multi_map.put('key', 'value')
        self.multi_map.clear()

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(event, event_type=EntryEventType.clear_all, number_of_affected_entries=1)

        self.assertTrueEventually(assert_event, 5)

    def test_add_entry_listener_with_key(self):
        collector = event_collector()
        id = self.multi_map.add_entry_listener(key='key1', include_value=True, added_func=collector)
        self.multi_map.put('key2', 'value2')
        self.multi_map.put('key1', 'value1')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(event, key='key1', event_type=EntryEventType.added, value='value1')

        self.assertTrueEventually(assert_event, 5)

    def test_clear(self):
        self._fill_map()

        self.multi_map.clear()

        self.assertEqual(self.multi_map.size(), 0)

    def test_contains_key(self):
        self._fill_map()

        self.assertTrue(self.multi_map.contains_key("key-1"))
        self.assertFalse(self.multi_map.contains_key("key-10"))

    def test_contains_value(self):
        self._fill_map()

        self.assertTrue(self.multi_map.contains_value("value-1-1"))
        self.assertFalse(self.multi_map.contains_value("value-10-10"))

    def test_contains_entry(self):
        self._fill_map()

        self.assertTrue(self.multi_map.contains_entry("key-1", "value-1-1"))
        self.assertFalse(self.multi_map.contains_entry("key-1", "value-1-10"))
        self.assertFalse(self.multi_map.contains_entry("key-10", "value-1-1"))

    def test_entry_set(self):
        mm = self._fill_map()

        entry_list = []
        for key, list in mm.iteritems():
            for value in list:
                entry_list.append((key, value))

        self.assertItemsEqual(self.multi_map.entry_set(), entry_list)

    def test_force_unlock(self):
        self.multi_map.put("key", "value")
        self.multi_map.lock("key")

        self.start_new_thread(lambda: self.multi_map.force_unlock("key"))

        self.assertTrueEventually(lambda: self.assertFalse(self.multi_map.is_locked("key")))

    def test_is_locked(self):
        self.multi_map.put("key", "value")

        self.assertFalse(self.multi_map.is_locked("key"))
        self.multi_map.lock("key")
        self.assertTrue(self.multi_map.is_locked("key"))
        self.multi_map.unlock("key")
        self.assertFalse(self.multi_map.is_locked("key"))

    def test_key_set(self):
        keys = self._fill_map().keys()

        self.assertItemsEqual(self.multi_map.key_set(), keys)

    def test_lock(self):
        self.multi_map.put("key", "value")

        t = self.start_new_thread(lambda: self.multi_map.lock("key"))
        t.join()

        self.assertFalse(self.multi_map.try_lock("key", timeout=0.01))

    def test_put_get(self):
        self.assertTrue(self.multi_map.put("key", "value1"))
        self.assertTrue(self.multi_map.put("key", "value2"))
        self.assertFalse(self.multi_map.put("key", "value2"))

        self.assertItemsEqual(self.multi_map.get("key"), ["value1", "value2"])

    def test_remove(self):
        self.multi_map.put("key", "value")

        self.assertTrue(self.multi_map.remove("key", "value"))
        self.assertFalse(self.multi_map.contains_key("key"))

    def test_remove_when_missing(self):
        self.multi_map.put("key", "value")
        self.assertFalse(self.multi_map.remove("key", "other_value"))

    def test_remove_all(self):
        self.multi_map.put("key", "value")
        self.multi_map.put("key", "value2")

        removed = self.multi_map.remove_all("key")

        self.assertItemsEqual(removed, ["value", "value2"])
        self.assertEqual(self.multi_map.size(), 0)

    def test_remove_entry_listener(self):
        collector = event_collector()
        id = self.multi_map.add_entry_listener(added_func=collector)

        self.multi_map.put('key', 'value')
        self.assertTrueEventually(lambda: self.assertEqual(len(collector.events), 1))
        self.multi_map.remove_entry_listener(id)
        self.multi_map.put('key2', 'value')

        time.sleep(1)
        self.assertEqual(len(collector.events), 1)

    def test_size(self):
        self._fill_map(5)

        self.assertEqual(25, self.multi_map.size())

    def test_try_lock_when_unlocked(self):
        self.assertTrue(self.multi_map.try_lock("key"))
        self.assertTrue(self.multi_map.is_locked("key"))

    def test_try_lock_when_locked(self):
        t = self.start_new_thread(lambda: self.multi_map.lock("key"))
        t.join()
        self.assertFalse(self.multi_map.try_lock("key", timeout=0.1))

    def test_unlock(self):
        self.multi_map.lock("key")
        self.assertTrue(self.multi_map.is_locked("key"))
        self.multi_map.unlock("key")
        self.assertFalse(self.multi_map.is_locked("key"))

    def test_unlock_when_no_lock(self):
        with self.assertRaises(HazelcastError):
            self.multi_map.unlock("key")

    def test_value_count(self):
        self._fill_map(key_count=1, value_count=10)

        self.assertEqual(self.multi_map.value_count("key-0"), 10)

    def test_values(self):
        values = self._fill_map().values()
        self.assertItemsEqual(self.multi_map.values(), itertools.chain(*values))

    def test_str(self):
        self.assertTrue(str(self.multi_map).startswith("MultiMap"))

    def _fill_map(self, key_count=5, value_count=5):
        map = {"key-%d" % x: ["value-%d-%d" % (x, y) for y in xrange(0, value_count)] for x in xrange(0, key_count)}
        for k, l in map.iteritems():
            for v in l:
                self.multi_map.put(k, v)

        return map