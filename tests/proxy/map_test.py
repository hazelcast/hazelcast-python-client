import time

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
        expected = self._fill_map(10)

        print(expected.keys())
        print(self.map.get_all(expected.keys()).result())
        actual = self.map.get_all(expected.keys()).result()

        self.assertItemsEqual(expected, actual)

    def test_is_locked(self):
        self.map.put("key", "value").result()

        self.assertFalse(self.map.is_locked("key").result())
        self.map.lock("key").result()
        self.assertTrue(self.map.is_locked("key").result())
        self.map.unlock("key").result()
        self.assertFalse(self.map.is_locked("key").result())

    def test_put_get(self):
        self._fill_map()
        for i in xrange(0, 10):
            self.assertEqual("value-%d" % i, self.map.get("key-%d" % i).result())

    def test_remove(self):
        self._fill_map()

        self.map.remove("key-1").result()
        self.assertEqual(9, self.map.size().result())
        self.assertFalse(self.map.contains_key("key-1").result())

    def test_remove_entry_listener(self):
        collector = event_collector()
        id = self.map.add_entry_listener(added=collector)

        self.map.put('key', 'value').result()
        self.assertTrueEventually(lambda: self.assertEqual(len(collector.events), 1))
        self.map.remove_entry_listener(id)
        self.map.put('key2', 'value').result()

        time.sleep(1)
        self.assertEqual(len(collector.events), 1)

    def test_size(self):
        self._fill_map()

        self.assertEqual(10, self.map.size().result())

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
