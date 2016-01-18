import time

from hazelcast.proxy.map import EntryEventType
from tests.base import SingleMemberTestCase
from tests.util import random_string, event_collector


class ClientMapTest(SingleMemberTestCase):
    def setUp(self):
        self.map = self.client.get_map(random_string())

    def tearDown(self):
        self.map.destroy()

    def fill_map(self, map):
        for i in xrange(0, 10):
            map.put("key-%d" % i, "value-%d" % i).result()

    def test_put_get(self):
        self.fill_map(self.map)
        for i in xrange(0, 10):
            self.assertEqual("value-%d" % i, self.map.get("key-%d" % i).result())

    def test_contains_key(self):
        self.fill_map(self.map)

        self.assertTrue(self.map.contains_key("key-1").result())
        self.assertFalse(self.map.contains_key("key-10").result())

    def test_size(self):
        self.fill_map(self.map)

        self.assertEqual(10, self.map.size().result())

    def test_remove(self):
        self.fill_map(self.map)

        self.map.remove("key-1").result()
        self.assertEqual(9, self.map.size().result())
        self.assertFalse(self.map.contains_key("key-1").result())

    def test_add_entry_listener_item_added(self):
        collector = event_collector()
        self.map.add_entry_listener(include_value=True, added=collector)
        self.map.put('key', 'value')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self._assert_entry_event(event, event_type=EntryEventType.added, value='value')

        self.assertTrueEventually(assert_event, 5)

    def test_add_entry_listener_item_removed(self):
        collector = event_collector()
        self.map.add_entry_listener(include_value=True, removed=collector)
        self.map.put('key', 'value')
        self.map.remove('key')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self._assert_entry_event(event, event_type=EntryEventType.removed, old_value='value')

        self.assertTrueEventually(assert_event, 5)

    def test_add_entry_listener_item_updated(self):
        collector = event_collector()
        self.map.add_entry_listener(include_value=True, updated=collector)
        self.map.put('key', 'value')
        self.map.put('key', 'new_value')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self._assert_entry_event(event, event_type=EntryEventType.updated, old_value='value', value='new_value')

        self.assertTrueEventually(assert_event, 5)

    def test_add_entry_listener_item_expired(self):
        collector = event_collector()
        self.map.add_entry_listener(include_value=True, expired=collector)
        self.map.put('key', 'value', ttl=0.1)

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self._assert_entry_event(event, event_type=EntryEventType.expired, old_value='value')

        self.assertTrueEventually(assert_event, 10)

    def test_remove_entry_listener(self):
        collector = event_collector()
        id = self.map.add_entry_listener(added=collector)

        self.map.put('key', 'value').result()
        self.assertTrueEventually(lambda: self.assertEqual(len(collector.events), 1))
        self.map.remove_entry_listener(id)
        self.map.put('key2', 'value').result()

        time.sleep(1)
        self.assertEqual(len(collector.events), 1)

    def _assert_entry_event(self, event, event_type, value=None, old_value=None, merging_value=None,
                            number_of_affected_entries=1):
        self.assertEqual(event.value, value)
        self.assertEqual(event.merging_value, merging_value)
        self.assertEqual(event.old_value, old_value)
        self.assertEqual(event.number_of_affected_entries, number_of_affected_entries)
        self.assertEquals(event.event_type, event_type)
