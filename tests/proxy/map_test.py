import time

from hazelcast.exception import HazelcastError
from hazelcast.proxy.map import EntryEventType
from tests.base import SingleMemberTestCase
from tests.util import random_string, event_collector


class ClientMapTest(SingleMemberTestCase):
    def setUp(self):
        self.map = self.client.get_map(random_string())

    def tearDown(self):
        self.map.destroy()

    def test_add_entry_listener_item_added(self):
        collector = event_collector()
        self.map.add_entry_listener(include_value=True, added=collector)
        self.map.put('key', 'value')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self._assert_entry_event(event, key='key', event_type=EntryEventType.added, value='value')

        self.assertTrueEventually(assert_event, 5)

    def test_add_entry_listener_item_removed(self):
        collector = event_collector()
        self.map.add_entry_listener(include_value=True, removed=collector)
        self.map.put('key', 'value')
        self.map.remove('key')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self._assert_entry_event(event, key='key', event_type=EntryEventType.removed, old_value='value')

        self.assertTrueEventually(assert_event, 5)

    def test_add_entry_listener_item_updated(self):
        collector = event_collector()
        self.map.add_entry_listener(include_value=True, updated=collector)
        self.map.put('key', 'value')
        self.map.put('key', 'new_value')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self._assert_entry_event(event, key='key', event_type=EntryEventType.updated, old_value='value',
                                     value='new_value')

        self.assertTrueEventually(assert_event, 5)

    def test_add_entry_listener_item_expired(self):
        collector = event_collector()
        self.map.add_entry_listener(include_value=True, expired=collector)
        self.map.put('key', 'value', ttl=0.1)

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self._assert_entry_event(event, key='key', event_type=EntryEventType.expired, old_value='value')

        self.assertTrueEventually(assert_event, 10)

    def test_add_entry_listener_with_key(self):
        collector = event_collector()
        id = self.map.add_entry_listener(key='key1', include_value=True, added=collector)
        self.map.put('key2', 'value2')
        self.map.put('key1', 'value1')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self._assert_entry_event(event, key='key1', event_type=EntryEventType.added, value='value1')

        self.assertTrueEventually(assert_event, 5)

    def test_add_index(self):
        self.map.add_index("field", True).result()

    def test_clear(self):
        self._fill_map()

        self.map.clear().result()

        self.assertEqual(self.map.size().result(), 0)

    def test_contains_key(self):
        self._fill_map()

        self.assertTrue(self.map.contains_key("key-1").result())
        self.assertFalse(self.map.contains_key("key-10").result())

    def test_contains_value(self):
        self._fill_map()

        self.assertTrue(self.map.contains_value("value-1").result())
        self.assertFalse(self.map.contains_value("value-10").result())

    def test_delete(self):
        self._fill_map()

        self.map.delete("key-1").result()

        self.assertEqual(self.map.size().result(), 9)
        self.assertFalse(self.map.contains_key("key-1").result())

    def test_entry_set(self):
        entries = self._fill_map()

        self.assertItemsEqual(self.map.entry_set().result(), list(entries.iteritems()))

    def test_evict(self):
        self._fill_map()

        self.map.evict("key-1").result()

        self.assertEqual(self.map.size().result(), 9)
        self.assertFalse(self.map.contains_key("key-1").result())

    def test_evict_all(self):
        self._fill_map()

        self.map.evict_all().result()

        self.assertEqual(self.map.size().result(), 0)

    def test_flush(self):
        self._fill_map()

        self.map.flush().result()

    def test_force_unlock(self):
        self.map.put("key", "value").result()
        self.map.lock("key").result()

        self.start_new_thread(lambda: self.map.force_unlock("key").result())

        self.assertTrueEventually(lambda: self.assertFalse(self.map.is_locked("key").result()))

    def test_get_all(self):
        expected = self._fill_map(1000)

        actual = self.map.get_all(expected.keys()).result()

        self.assertItemsEqual(expected, actual)

    def test_get_all_when_no_keys(self):
        self.assertEqual(self.map.get_all([]).result(), {})

    def test_get_entry_view(self):
        self.map.put("key", "value").result()
        self.map.get("key").result()
        self.map.put("key", "new_value").result()

        entry_view = self.map.get_entry_view("key").result()

        self.assertEqual(entry_view.key, "key")
        self.assertEqual(entry_view.value, "new_value")
        self.assertIsNotNone(entry_view.creation_time)
        self.assertIsNotNone(entry_view.expiration_time)
        self.assertEqual(entry_view.hits, 2)
        self.assertEqual(entry_view.version, 1)
        self.assertEqual(entry_view.eviction_criteria_number, 0)
        self.assertIsNotNone(entry_view.last_access_time)
        self.assertIsNotNone(entry_view.last_stored_time)
        self.assertIsNotNone(entry_view.last_update_time)
        self.assertIsNotNone(entry_view.ttl)

    def test_is_empty(self):
        self.map.put("key", "value")

        self.assertFalse(self.map.is_empty().result())

        self.map.clear().result()

        self.assertTrue(self.map.is_empty().result())

    def test_is_locked(self):
        self.map.put("key", "value").result()

        self.assertFalse(self.map.is_locked("key").result())
        self.map.lock("key").result()
        self.assertTrue(self.map.is_locked("key").result())
        self.map.unlock("key").result()
        self.assertFalse(self.map.is_locked("key").result())

    def test_key_set(self):
        keys = self._fill_map().keys()

        self.assertItemsEqual(self.map.key_set().result(), keys)

    def test_load_all(self):
        keys = self._fill_map().keys()
        # TODO: needs map store configuration
        with self.assertRaises(HazelcastError):
            self.map.load_all().result()

    def test_lock(self):
        self.map.put("key", "value").result()

        t = self.start_new_thread(lambda: self.map.lock("key").result())
        t.join()

        self.assertFalse(self.map.try_put("key", "new_value", timeout=0.01).result())

    def test_put_all(self):
        map = {"key-%d" % x: "value-%d" % x for x in xrange(0, 1000)}
        self.map.put_all(map).result()

        entries = self.map.entry_set().result()

        self.assertItemsEqual(entries, map.iteritems())

    def test_put_all_when_no_keys(self):
        self.assertIsNone(self.map.put_all({}).result())

    def test_put_if_absent_when_missing_value(self):
        returned_value = self.map.put_if_absent("key", "new_value").result()

        self.assertIsNone(returned_value)
        self.assertEqual(self.map.get("key").result(), "new_value")

    def test_put_if_absent_when_existing_value(self):
        self.map.put("key", "value").result()

        returned_value = self.map.put_if_absent("key", "new_value").result()

        self.assertEqual(returned_value, "value")
        self.assertEqual(self.map.get("key").result(), "value")

    def test_put_get(self):
        self.assertIsNone(self.map.put("key", "value").result())
        self.assertEqual(self.map.get("key").result(), "value")

    def test_put_when_existing(self):
        self.map.put("key", "value").result()
        self.assertEqual(self.map.put("key", "new_value").result(), "value")
        self.assertEqual(self.map.get("key").result(), "new_value")

    def test_put_transient(self):
        self.map.put_transient("key", "value").result()

        self.assertEqual(self.map.get("key").result(), "value")

    def test_remove(self):
        self.map.put("key", "value")

        removed = self.map.remove("key").result()
        self.assertEqual(removed, "value")
        self.assertEqual(0, self.map.size().result())
        self.assertFalse(self.map.contains_key("key").result())

    def test_remove_if_same_when_same(self):
        self.map.put("key", "value")

        self.assertTrue(self.map.remove_if_same("key", "value").result())
        self.assertFalse(self.map.contains_key("key").result())

    def test_remove_if_same_when_different(self):
        self.map.put("key", "value")

        self.assertFalse(self.map.remove_if_same("key", "another_value").result())
        self.assertTrue(self.map.contains_key("key").result())

    def test_remove_entry_listener(self):
        collector = event_collector()
        id = self.map.add_entry_listener(added=collector)

        self.map.put('key', 'value').result()
        self.assertTrueEventually(lambda: self.assertEqual(len(collector.events), 1))
        self.map.remove_entry_listener(id)
        self.map.put('key2', 'value').result()

        time.sleep(1)
        self.assertEqual(len(collector.events), 1)

    def test_replace(self):
        self.map.put("key", "value")

        replaced = self.map.replace("key", "new_value").result()
        self.assertEqual(replaced, "value")
        self.assertEqual(self.map.get("key").result(), "new_value")

    def test_replace_if_same_when_same(self):
        self.map.put("key", "value")

        self.assertTrue(self.map.replace_if_same("key", "value", "new_value").result())
        self.assertEqual(self.map.get("key").result(), "new_value")

    def test_replace_if_same_when_different(self):
        self.map.put("key", "value")

        self.assertFalse(self.map.replace_if_same("key", "another_value", "new_value").result())
        self.assertEqual(self.map.get("key").result(), "value")

    def test_set(self):
        self.map.set("key", "value").result()

        self.assertEqual(self.map.get("key").result(), "value")

    def test_size(self):
        self._fill_map()

        self.assertEqual(10, self.map.size().result())

    def test_try_lock_when_unlocked(self):
        self.assertTrue(self.map.try_lock("key").result())
        self.assertTrue(self.map.is_locked("key").result())

    def test_try_lock_when_locked(self):
        t = self.start_new_thread(lambda: self.map.lock("key").result())
        t.join()
        self.assertFalse(self.map.try_lock("key", timeout=0.1).result())

    def test_try_put_when_unlocked(self):
        self.assertTrue(self.map.try_put("key", "value").result())
        self.assertEqual(self.map.get("key").result(), "value")

    def test_try_put_when_locked(self):
        t = self.start_new_thread(lambda: self.map.lock("key").result())
        t.join()
        self.assertFalse(self.map.try_put("key", "value", timeout=0.1).result())

    def test_try_remove_when_unlocked(self):
        self.map.put("key", "value").result()
        self.assertTrue(self.map.try_remove("key").result())
        self.assertIsNone(self.map.get("key").result())

    def test_try_remove_when_locked(self):
        self.map.put("key", "value").result()
        t = self.start_new_thread(lambda: self.map.lock("key").result())
        t.join()
        self.assertFalse(self.map.try_remove("key", timeout=0.1).result())

    def test_unlock(self):
        self.map.lock("key").result()
        self.assertTrue(self.map.is_locked("key").result())
        self.map.unlock("key").result()
        self.assertFalse(self.map.is_locked("key").result())

    def test_unlock_when_no_lock(self):
        with self.assertRaises(HazelcastError):
            self.map.unlock("key").result()

    def test_values(self):
        values = self._fill_map().values()

        self.assertItemsEqual(self.map.values().result(), values)

    def _fill_map(self, count=10):
        map = {"key-%d" % x: "value-%d" % x for x in xrange(0, count)}
        for k, v in map.iteritems():
            self.map.put(k, v).result()
        return map

    def _assert_entry_event(self, event, key, event_type, value=None, old_value=None, merging_value=None,
                            number_of_affected_entries=1):
        self.assertEqual(event.key, key)
        self.assertEquals(event.event_type, event_type)
        self.assertEqual(event.value, value)
        self.assertEqual(event.merging_value, merging_value)
        self.assertEqual(event.old_value, old_value)
        self.assertEqual(event.number_of_affected_entries, number_of_affected_entries)
