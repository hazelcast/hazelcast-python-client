import time
from hazelcast.exception import HazelcastError, HazelcastSerializationError
from hazelcast.proxy.map import EntryEventType
from hazelcast.serialization.api import IdentifiedDataSerializable
from hazelcast.serialization.predicate import SqlPredicate
from tests.base import SingleMemberTestCase
from tests.util import random_string, event_collector

FACTORY_ID = 1


class EntryProcessor(IdentifiedDataSerializable):
    CLASS_ID = 1

    def write_data(self, object_data_output):
        pass

    def get_factory_id(self):
        return FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID


class MapTest(SingleMemberTestCase):
    def setUp(self):
        self.map = self.client.get_map(random_string()).blocking()

    def tearDown(self):
        self.map.destroy()

    def test_add_entry_listener_item_added(self):
        collector = event_collector()
        self.map.add_entry_listener(include_value=True, added_func=collector)
        self.map.put('key', 'value')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(event, key='key', event_type=EntryEventType.added, value='value')

        self.assertTrueEventually(assert_event, 5)

    def test_add_entry_listener_item_removed(self):
        collector = event_collector()
        self.map.add_entry_listener(include_value=True, removed_func=collector)
        self.map.put('key', 'value')
        self.map.remove('key')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(event, key='key', event_type=EntryEventType.removed, old_value='value')

        self.assertTrueEventually(assert_event, 5)

    def test_add_entry_listener_item_updated(self):
        collector = event_collector()
        self.map.add_entry_listener(include_value=True, updated_func=collector)
        self.map.put('key', 'value')
        self.map.put('key', 'new_value')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(event, key='key', event_type=EntryEventType.updated, old_value='value',
                                  value='new_value')

        self.assertTrueEventually(assert_event, 5)

    def test_add_entry_listener_item_expired(self):
        collector = event_collector()
        self.map.add_entry_listener(include_value=True, expired_func=collector)
        self.map.put('key', 'value', ttl=0.1)

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(event, key='key', event_type=EntryEventType.expired, old_value='value')

        self.assertTrueEventually(assert_event, 10)

    def test_add_entry_listener_with_key(self):
        collector = event_collector()
        self.map.add_entry_listener(key='key1', include_value=True, added_func=collector)
        self.map.put('key2', 'value2')
        self.map.put('key1', 'value1')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(event, key='key1', event_type=EntryEventType.added, value='value1')

        self.assertTrueEventually(assert_event, 5)

    def test_add_entry_listener_with_predicate(self):
        collector = event_collector()
        self.map.add_entry_listener(predicate=SqlPredicate("this == value1"), include_value=True, added_func=collector)
        self.map.put('key2', 'value2')
        self.map.put('key1', 'value1')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(event, key='key1', event_type=EntryEventType.added, value='value1')

        self.assertTrueEventually(assert_event, 5)

    def test_add_entry_listener_with_key_and_predicate(self):
        collector = event_collector()
        self.map.add_entry_listener(key='key1', predicate=SqlPredicate("this == value3"), include_value=True, added_func=collector)
        self.map.put('key2', 'value2')
        self.map.put('key1', 'value1')
        self.map.remove('key1')
        self.map.put('key1', 'value3')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(event, key='key1', event_type=EntryEventType.added, value='value3')

        self.assertTrueEventually(assert_event, 5)

    def test_add_index(self):
        self.map.add_index("field", True)

    def test_clear(self):
        self._fill_map()

        self.map.clear()

        self.assertEqual(self.map.size(), 0)

    def test_contains_key(self):
        self._fill_map()

        self.assertTrue(self.map.contains_key("key-1"))
        self.assertFalse(self.map.contains_key("key-10"))

    def test_contains_value(self):
        self._fill_map()

        self.assertTrue(self.map.contains_value("value-1"))
        self.assertFalse(self.map.contains_value("value-10"))

    def test_delete(self):
        self._fill_map()

        self.map.delete("key-1")

        self.assertEqual(self.map.size(), 9)
        self.assertFalse(self.map.contains_key("key-1"))

    def test_entry_set(self):
        entries = self._fill_map()

        self.assertItemsEqual(self.map.entry_set(), list(entries.iteritems()))

    def test_entry_set_with_predicate(self):
        self._fill_map()

        self.assertEqual(self.map.entry_set(SqlPredicate("this == 'value-1'")), [("key-1", "value-1")])

    def test_evict(self):
        self._fill_map()

        self.map.evict("key-1")

        self.assertEqual(self.map.size(), 9)
        self.assertFalse(self.map.contains_key("key-1"))

    def test_evict_all(self):
        self._fill_map()

        self.map.evict_all()

        self.assertEqual(self.map.size(), 0)

    def test_execute_on_entries(self):
        # TODO: EntryProcessor must be defined on the server
        with self.assertRaises(HazelcastSerializationError):
            self.map.execute_on_entries(EntryProcessor())

    def test_execute_on_entries_with_predicate(self):
        # TODO: EntryProcessor must be defined on the server
        with self.assertRaises(HazelcastSerializationError):
            self.map.execute_on_entries(EntryProcessor(), predicate=SqlPredicate())

    def test_execute_on_key(self):
        # TODO: EntryProcessor must be defined on the server
        self.map.put("key", 1)
        with self.assertRaises(HazelcastSerializationError):
            self.map.execute_on_key("key", EntryProcessor())

    def test_execute_on_keys(self):
        # TODO: EntryProcessor must be defined on the server
        map = self._fill_map()
        with self.assertRaises(HazelcastSerializationError):
            self.map.execute_on_keys(map.keys(), EntryProcessor())

    def test_flush(self):
        self._fill_map()

        self.map.flush()

    def test_force_unlock(self):
        self.map.put("key", "value")
        self.map.lock("key")

        self.start_new_thread(lambda: self.map.force_unlock("key"))

        self.assertTrueEventually(lambda: self.assertFalse(self.map.is_locked("key")))

    def test_get_all(self):
        expected = self._fill_map(1000)

        actual = self.map.get_all(expected.keys())

        self.assertItemsEqual(expected, actual)

    def test_get_all_when_no_keys(self):
        self.assertEqual(self.map.get_all([]), {})

    def test_get_entry_view(self):
        self.map.put("key", "value")
        self.map.get("key")
        self.map.put("key", "new_value")

        entry_view = self.map.get_entry_view("key")

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

        self.assertFalse(self.map.is_empty())

        self.map.clear()

        self.assertTrue(self.map.is_empty())

    def test_is_locked(self):
        self.map.put("key", "value")

        self.assertFalse(self.map.is_locked("key"))
        self.map.lock("key")
        self.assertTrue(self.map.is_locked("key"))
        self.map.unlock("key")
        self.assertFalse(self.map.is_locked("key"))

    def test_key_set(self):
        keys = self._fill_map().keys()

        self.assertItemsEqual(self.map.key_set(), keys)

    def test_key_set_with_predicate(self):
        self._fill_map()

        self.assertEqual(self.map.key_set(SqlPredicate("this == 'value-1'")), ["key-1"])

    def test_load_all(self):
        keys = self._fill_map().keys()
        # TODO: needs map store configuration
        with self.assertRaises(HazelcastError):
            self.map.load_all()

    def test_load_all_with_keys(self):
        keys = self._fill_map().keys()
        # TODO: needs map store configuration
        with self.assertRaises(HazelcastError):
            self.map.load_all(["key-1", "key-2"])

    def test_lock(self):
        self.map.put("key", "value")

        t = self.start_new_thread(lambda: self.map.lock("key"))
        t.join()

        self.assertFalse(self.map.try_put("key", "new_value", timeout=0.01))

    def test_put_all(self):
        map = {"key-%d" % x: "value-%d" % x for x in xrange(0, 1000)}
        self.map.put_all(map)

        entries = self.map.entry_set()

        self.assertItemsEqual(entries, map.iteritems())

    def test_put_all_when_no_keys(self):
        self.assertIsNone(self.map.put_all({}))

    def test_put_if_absent_when_missing_value(self):
        returned_value = self.map.put_if_absent("key", "new_value")

        self.assertIsNone(returned_value)
        self.assertEqual(self.map.get("key"), "new_value")

    def test_put_if_absent_when_existing_value(self):
        self.map.put("key", "value")

        returned_value = self.map.put_if_absent("key", "new_value")

        self.assertEqual(returned_value, "value")
        self.assertEqual(self.map.get("key"), "value")

    def test_put_get(self):
        self.assertIsNone(self.map.put("key", "value"))
        self.assertEqual(self.map.get("key"), "value")

    def test_put_get2(self):
        val = "x"*5000

        self.assertIsNone(self.map.put("key-x", val))
        self.assertEqual(self.map.get("key-x"), val)

    def test_put_when_existing(self):
        self.map.put("key", "value")
        self.assertEqual(self.map.put("key", "new_value"), "value")
        self.assertEqual(self.map.get("key"), "new_value")

    def test_put_transient(self):
        self.map.put_transient("key", "value")

        self.assertEqual(self.map.get("key"), "value")

    def test_remove(self):
        self.map.put("key", "value")

        removed = self.map.remove("key")
        self.assertEqual(removed, "value")
        self.assertEqual(0, self.map.size())
        self.assertFalse(self.map.contains_key("key"))

    def test_remove_if_same_when_same(self):
        self.map.put("key", "value")

        self.assertTrue(self.map.remove_if_same("key", "value"))
        self.assertFalse(self.map.contains_key("key"))

    def test_remove_if_same_when_different(self):
        self.map.put("key", "value")

        self.assertFalse(self.map.remove_if_same("key", "another_value"))
        self.assertTrue(self.map.contains_key("key"))

    def test_remove_entry_listener(self):
        collector = event_collector()
        id = self.map.add_entry_listener(added_func=collector)

        self.map.put('key', 'value')
        self.assertTrueEventually(lambda: self.assertEqual(len(collector.events), 1))
        self.map.remove_entry_listener(id)
        self.map.put('key2', 'value')

        time.sleep(1)
        self.assertEqual(len(collector.events), 1)

    def test_replace(self):
        self.map.put("key", "value")

        replaced = self.map.replace("key", "new_value")
        self.assertEqual(replaced, "value")
        self.assertEqual(self.map.get("key"), "new_value")

    def test_replace_if_same_when_same(self):
        self.map.put("key", "value")

        self.assertTrue(self.map.replace_if_same("key", "value", "new_value"))
        self.assertEqual(self.map.get("key"), "new_value")

    def test_replace_if_same_when_different(self):
        self.map.put("key", "value")

        self.assertFalse(self.map.replace_if_same("key", "another_value", "new_value"))
        self.assertEqual(self.map.get("key"), "value")

    def test_set(self):
        self.map.set("key", "value")

        self.assertEqual(self.map.get("key"), "value")

    def test_size(self):
        self._fill_map()

        self.assertEqual(10, self.map.size())

    def test_try_lock_when_unlocked(self):
        self.assertTrue(self.map.try_lock("key"))
        self.assertTrue(self.map.is_locked("key"))

    def test_try_lock_when_locked(self):
        t = self.start_new_thread(lambda: self.map.lock("key"))
        t.join()
        self.assertFalse(self.map.try_lock("key", timeout=0.1))

    def test_try_put_when_unlocked(self):
        self.assertTrue(self.map.try_put("key", "value"))
        self.assertEqual(self.map.get("key"), "value")

    def test_try_put_when_locked(self):
        t = self.start_new_thread(lambda: self.map.lock("key"))
        t.join()
        self.assertFalse(self.map.try_put("key", "value", timeout=0.1))

    def test_try_remove_when_unlocked(self):
        self.map.put("key", "value")
        self.assertTrue(self.map.try_remove("key"))
        self.assertIsNone(self.map.get("key"))

    def test_try_remove_when_locked(self):
        self.map.put("key", "value")
        t = self.start_new_thread(lambda: self.map.lock("key"))
        t.join()
        self.assertFalse(self.map.try_remove("key", timeout=0.1))

    def test_unlock(self):
        self.map.lock("key")
        self.assertTrue(self.map.is_locked("key"))
        self.map.unlock("key")
        self.assertFalse(self.map.is_locked("key"))

    def test_unlock_when_no_lock(self):
        with self.assertRaises(HazelcastError):
            self.map.unlock("key")

    def test_values(self):
        values = self._fill_map().values()

        self.assertItemsEqual(self.map.values(), values)

    def test_values_with_predicate(self):
        self._fill_map()

        self.assertEqual(self.map.values(SqlPredicate("this == 'value-1'")), ["value-1"])

    def test_str(self):
        self.assertTrue(str(self.map).startswith("Map"))

    def _fill_map(self, count=10):
        map = {"key-%d" % x: "value-%d" % x for x in xrange(0, count)}
        for k, v in map.iteritems():
            self.map.put(k, v)
        return map
