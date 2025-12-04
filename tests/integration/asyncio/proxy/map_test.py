import asyncio
import os
import time
import unittest


try:
    from hazelcast.aggregator import (
        count,
        distinct,
        double_avg,
        double_sum,
        fixed_point_sum,
        floating_point_sum,
        int_avg,
        int_sum,
        long_avg,
        long_sum,
        max_,
        max_by,
        min_,
        min_by,
        number_avg,
    )
except ImportError:
    # If the import of those fail, we won't use
    # them in the tests thanks to client version check.
    pass

try:
    from hazelcast.projection import (
        single_attribute,
        multi_attribute,
        identity,
    )
except ImportError:
    # If the import of those fail, we won't use
    # them in the tests thanks to client version check.
    pass

from hazelcast.core import HazelcastJsonValue
from hazelcast.config import IndexType, IntType
from hazelcast.errors import HazelcastError
from hazelcast.predicate import greater_or_equal, less_or_equal, sql, paging, true
from hazelcast.internal.asyncio_proxy.map import EntryEventType
from hazelcast.serialization.api import IdentifiedDataSerializable
from tests.integration.asyncio.base import SingleMemberTestCase
from tests.integration.backward_compatible.util import (
    read_string_from_input,
    write_string_to_output,
)
from tests.util import (
    event_collector,
    get_current_timestamp,
    compare_client_version,
    compare_server_version,
    skip_if_client_version_older_than,
    random_string,
)

from tests.integration.asyncio.util import fill_map


class EntryProcessor(IdentifiedDataSerializable):
    FACTORY_ID = 66
    CLASS_ID = 1

    def __init__(self, value=None):
        self.value = value

    def write_data(self, object_data_output):
        write_string_to_output(object_data_output, self.value)

    def read_data(self, object_data_input):
        self.value = read_string_from_input(object_data_input)

    def get_factory_id(self):
        return self.FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID


class MapGetInterceptor(IdentifiedDataSerializable):

    FACTORY_ID = 666
    CLASS_ID = 6

    def __init__(self, prefix):
        self.prefix = prefix

    def write_data(self, object_data_output):
        write_string_to_output(object_data_output, self.prefix)

    def read_data(self, object_data_input):
        pass

    def get_factory_id(self):
        return self.FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID


class MapTest(SingleMemberTestCase):
    @classmethod
    def configure_cluster(cls):
        path = os.path.abspath(__file__)
        dir_path = os.path.dirname(path)
        with open(os.path.join(dir_path, "../../backward_compatible/proxy/hazelcast.xml")) as f:
            return f.read()

    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        config["data_serializable_factories"] = {
            EntryProcessor.FACTORY_ID: {EntryProcessor.CLASS_ID: EntryProcessor}
        }
        return config

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.map = await self.client.get_map(random_string())

    async def asyncTearDown(self):
        await self.map.destroy()
        await super().asyncTearDown()

    async def test_add_entry_listener_item_added(self):
        collector = event_collector()
        await self.map.add_entry_listener(include_value=True, added_func=collector)
        await self.map.put("key", "value")

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(event, key="key", event_type=EntryEventType.ADDED, value="value")

        await self.assertTrueEventually(assert_event, 5)

    async def test_add_entry_listener_item_removed(self):
        collector = event_collector()
        await self.map.add_entry_listener(include_value=True, removed_func=collector)
        await self.map.put("key", "value")
        await self.map.remove("key")

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(
                event, key="key", event_type=EntryEventType.REMOVED, old_value="value"
            )

        await self.assertTrueEventually(assert_event, 5)

    async def test_add_entry_listener_item_updated(self):
        collector = event_collector()
        await self.map.add_entry_listener(include_value=True, updated_func=collector)
        await self.map.put("key", "value")
        await self.map.put("key", "new_value")

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(
                event,
                key="key",
                event_type=EntryEventType.UPDATED,
                old_value="value",
                value="new_value",
            )

        await self.assertTrueEventually(assert_event, 5)

    async def test_add_entry_listener_item_expired(self):
        collector = event_collector()
        await self.map.add_entry_listener(include_value=True, expired_func=collector)
        await self.map.put("key", "value", ttl=0.1)

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(
                event, key="key", event_type=EntryEventType.EXPIRED, old_value="value"
            )

        await self.assertTrueEventually(assert_event, 10)

    async def test_add_entry_listener_with_key(self):
        collector = event_collector()
        await self.map.add_entry_listener(key="key1", include_value=True, added_func=collector)
        await self.map.put("key2", "value2")
        await self.map.put("key1", "value1")

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(
                event, key="key1", event_type=EntryEventType.ADDED, value="value1"
            )

        await self.assertTrueEventually(assert_event, 5)

    async def test_add_entry_listener_with_predicate(self):
        collector = event_collector()
        await self.map.add_entry_listener(
            predicate=sql("this == value1"), include_value=True, added_func=collector
        )
        await self.map.put("key2", "value2")
        await self.map.put("key1", "value1")

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(
                event, key="key1", event_type=EntryEventType.ADDED, value="value1"
            )

        await self.assertTrueEventually(assert_event, 5)

    async def test_add_entry_listener_with_key_and_predicate(self):
        collector = event_collector()
        await self.map.add_entry_listener(
            key="key1", predicate=sql("this == value3"), include_value=True, added_func=collector
        )
        await self.map.put("key2", "value2")
        await self.map.put("key1", "value1")
        await self.map.remove("key1")
        await self.map.put("key1", "value3")

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(
                event, key="key1", event_type=EntryEventType.ADDED, value="value3"
            )

        await self.assertTrueEventually(assert_event, 5)

    async def test_add_index(self):
        await self.map.add_index(attributes=["this"])
        await self.map.add_index(attributes=["this"], index_type=IndexType.HASH)
        await self.map.add_index(
            attributes=["this"],
            index_type=IndexType.BITMAP,
            bitmap_index_options={
                "unique_key": "this",
            },
        )

    async def test_add_index_duplicate_fields(self):
        with self.assertRaises(ValueError):
            await self.map.add_index(attributes=["this", "this"])

    async def test_add_index_invalid_attribute(self):
        with self.assertRaises(ValueError):
            await self.map.add_index(attributes=["this.x."])

    async def test_clear(self):
        await self.fill_map()
        await self.map.clear()
        self.assertEqual(await self.map.size(), 0)

    async def test_contains_key(self):
        await self.fill_map()
        self.assertTrue(await self.map.contains_key("key-1"))
        self.assertFalse(await self.map.contains_key("key-10"))

    async def test_contains_value(self):
        await self.fill_map()
        self.assertTrue(await self.map.contains_value("value-1"))
        self.assertFalse(await self.map.contains_value("value-10"))

    async def test_delete(self):
        await self.fill_map()
        await self.map.delete("key-1")
        self.assertEqual(await self.map.size(), 9)
        self.assertFalse(await self.map.contains_key("key-1"))

    async def test_entry_set(self):
        entries = await self.fill_map()
        self.assertCountEqual(await self.map.entry_set(), list(entries.items()))

    async def test_entry_set_with_predicate(self):
        await self.fill_map()
        self.assertEqual(await self.map.entry_set(sql("this == 'value-1'")), [("key-1", "value-1")])

    async def test_evict(self):
        await self.fill_map()
        await self.map.evict("key-1")
        self.assertEqual(await self.map.size(), 9)
        self.assertFalse(await self.map.contains_key("key-1"))

    async def test_evict_all(self):
        await self.fill_map()
        await self.map.evict_all()
        self.assertEqual(await self.map.size(), 0)

    async def test_execute_on_entries(self):
        m = await self.fill_map()
        expected_entry_set = [(key, "processed") for key in m]
        values = await self.map.execute_on_entries(EntryProcessor("processed"))

        self.assertCountEqual(expected_entry_set, await self.map.entry_set())
        self.assertCountEqual(expected_entry_set, values)

    async def test_execute_on_entries_with_predicate(self):
        m = await self.fill_map()
        expected_entry_set = [(key, "processed") if key < "key-5" else (key, m[key]) for key in m]
        expected_values = [(key, "processed") for key in m if key < "key-5"]
        values = await self.map.execute_on_entries(
            EntryProcessor("processed"), sql("__key < 'key-5'")
        )
        self.assertCountEqual(expected_entry_set, await self.map.entry_set())
        self.assertCountEqual(expected_values, values)

    async def test_execute_on_key(self):
        await self.map.put("test-key", "test-value")
        value = await self.map.execute_on_key("test-key", EntryProcessor("processed"))
        self.assertEqual("processed", await self.map.get("test-key"))
        self.assertEqual("processed", value)

    async def test_execute_on_keys(self):
        m = await self.fill_map()
        expected_entry_set = [(key, "processed") for key in m]
        values = await self.map.execute_on_keys(list(m.keys()), EntryProcessor("processed"))
        self.assertCountEqual(expected_entry_set, await self.map.entry_set())
        self.assertCountEqual(expected_entry_set, values)

    async def test_execute_on_keys_with_empty_key_list(self):
        m = await self.fill_map()
        expected_entry_set = [(key, m[key]) for key in m]
        values = await self.map.execute_on_keys([], EntryProcessor("processed"))
        self.assertEqual([], values)
        self.assertCountEqual(expected_entry_set, await self.map.entry_set())

    async def test_flush(self):
        await self.fill_map()
        await self.map.flush()

    async def test_get_all(self):
        expected = await self.fill_map(1000)
        actual = await self.map.get_all(list(expected.keys()))
        self.assertCountEqual(expected, actual)

    async def test_get_all_when_no_keys(self):
        self.assertEqual(await self.map.get_all([]), {})

    async def test_get_entry_view(self):
        await self.map.put("key", "value")
        await self.map.get("key")
        await self.map.put("key", "new_value")

        entry_view = await self.map.get_entry_view("key")

        self.assertEqual(entry_view.key, "key")
        self.assertEqual(entry_view.value, "new_value")
        self.assertIsNotNone(entry_view.cost)
        self.assertIsNotNone(entry_view.creation_time)
        self.assertIsNotNone(entry_view.expiration_time)
        if compare_server_version(self.client, "4.2") < 0:
            self.assertEqual(entry_view.hits, 2)
        else:
            # 4.2+ servers do not collect per entry stats by default
            self.assertIsNotNone(entry_view.hits)
        self.assertIsNotNone(entry_view.last_access_time)
        self.assertIsNotNone(entry_view.last_stored_time)
        self.assertIsNotNone(entry_view.last_update_time)
        self.assertEqual(entry_view.version, 1)
        self.assertIsNotNone(entry_view.ttl)
        self.assertIsNotNone(entry_view.max_idle)

    async def test_is_empty(self):
        await self.map.put("key", "value")
        self.assertFalse(await self.map.is_empty())
        await self.map.clear()
        self.assertTrue(await self.map.is_empty())

    async def test_key_set(self):
        keys = list((await self.fill_map()).keys())
        self.assertCountEqual(await self.map.key_set(), keys)

    async def test_key_set_with_predicate(self):
        await self.fill_map()
        self.assertEqual(await self.map.key_set(sql("this == 'value-1'")), ["key-1"])

    async def test_put_all(self):
        m = {"key-%d" % x: "value-%d" % x for x in range(0, 1000)}
        await self.map.put_all(m)

        entries = await self.map.entry_set()

        self.assertCountEqual(entries, m.items())

    async def test_put_all_when_no_keys(self):
        self.assertIsNone(await self.map.put_all({}))

    async def test_put_if_absent_when_missing_value(self):
        returned_value = await self.map.put_if_absent("key", "new_value")

        self.assertIsNone(returned_value)
        self.assertEqual(await self.map.get("key"), "new_value")

    async def test_put_if_absent_when_existing_value(self):
        await self.map.put("key", "value")
        returned_value = await self.map.put_if_absent("key", "new_value")
        self.assertEqual(returned_value, "value")
        self.assertEqual(await self.map.get("key"), "value")

    async def test_put_get(self):
        self.assertIsNone(await self.map.put("key", "value"))
        self.assertEqual(await self.map.get("key"), "value")

    async def test_put_get_large_payload(self):
        # The fix for reading large payloads is introduced in 4.2.1
        # See https://github.com/hazelcast/hazelcast-python-client/pull/436
        skip_if_client_version_older_than(self, "4.2.1")

        payload = bytearray(os.urandom(16 * 1024 * 1024))
        start = get_current_timestamp()
        self.assertIsNone(await self.map.put("key", payload))
        self.assertEqual(await self.map.get("key"), payload)
        self.assertLessEqual(get_current_timestamp() - start, 5)

    async def test_put_get2(self):
        val = "x" * 5000
        self.assertIsNone(await self.map.put("key-x", val))
        self.assertEqual(await self.map.get("key-x"), val)

    async def test_put_when_existing(self):
        await self.map.put("key", "value")
        self.assertEqual(await self.map.put("key", "new_value"), "value")
        self.assertEqual(await self.map.get("key"), "new_value")

    async def test_put_transient(self):
        await self.map.put_transient("key", "value")
        self.assertEqual(await self.map.get("key"), "value")

    async def test_remove(self):
        await self.map.put("key", "value")
        removed = await self.map.remove("key")
        self.assertEqual(removed, "value")
        self.assertEqual(0, await self.map.size())
        self.assertFalse(await self.map.contains_key("key"))

    async def test_remove_all_with_none_predicate(self):
        skip_if_client_version_older_than(self, "5.2.0")

        with self.assertRaises(AssertionError):
            await self.map.remove_all(None)

    async def test_remove_all(self):
        skip_if_client_version_older_than(self, "5.2.0")

        await self.fill_map()
        await self.map.remove_all(predicate=sql("__key > 'key-7'"))
        self.assertEqual(await self.map.size(), 8)

    async def test_remove_if_same_when_same(self):
        await self.map.put("key", "value")
        self.assertTrue(await self.map.remove_if_same("key", "value"))
        self.assertFalse(await self.map.contains_key("key"))

    async def test_remove_if_same_when_different(self):
        await self.map.put("key", "value")
        self.assertFalse(await self.map.remove_if_same("key", "another_value"))
        self.assertTrue(await self.map.contains_key("key"))

    async def test_remove_entry_listener(self):
        collector = event_collector()
        reg_id = await self.map.add_entry_listener(added_func=collector)

        await self.map.put("key", "value")
        await self.assertTrueEventually(lambda: self.assertEqual(len(collector.events), 1))
        await self.map.remove_entry_listener(reg_id)
        await self.map.put("key2", "value")

        await asyncio.sleep(1)
        self.assertEqual(len(collector.events), 1)

    async def test_remove_entry_listener_with_none_id(self):
        with self.assertRaises(AssertionError) as cm:
            await self.map.remove_entry_listener(None)
        e = cm.exception
        self.assertEqual(e.args[0], "None user_registration_id is not allowed!")

    async def test_replace(self):
        await self.map.put("key", "value")
        replaced = await self.map.replace("key", "new_value")
        self.assertEqual(replaced, "value")
        self.assertEqual(await self.map.get("key"), "new_value")

    async def test_replace_if_same_when_same(self):
        await self.map.put("key", "value")
        self.assertTrue(await self.map.replace_if_same("key", "value", "new_value"))
        self.assertEqual(await self.map.get("key"), "new_value")

    async def test_replace_if_same_when_different(self):
        await self.map.put("key", "value")
        self.assertFalse(await self.map.replace_if_same("key", "another_value", "new_value"))
        self.assertEqual(await self.map.get("key"), "value")

    async def test_set(self):
        await self.map.set("key", "value")

        self.assertEqual(await self.map.get("key"), "value")

    async def test_set_ttl(self):
        await self.map.put("key", "value")
        await self.map.set_ttl("key", 0.1)

        async def evicted():
            self.assertFalse(await self.map.contains_key("key"))

        await self.assertTrueEventually(evicted, 5)

    async def test_size(self):
        await self.fill_map()

        self.assertEqual(10, await self.map.size())

    async def test_values(self):
        values = list((await self.fill_map()).values())

        self.assertCountEqual(list(await self.map.values()), values)

    async def test_values_with_predicate(self):
        await self.fill_map()
        self.assertEqual(await self.map.values(sql("this == 'value-1'")), ["value-1"])

    def test_str(self):
        self.assertTrue(str(self.map).startswith("Map"))

    async def test_add_interceptor(self):
        interceptor = MapGetInterceptor(":")
        registration_id = await self.map.add_interceptor(interceptor)
        self.assertIsNotNone(registration_id)

        await self.map.set(1, ")")
        value = await self.map.get(1)
        self.assertEqual(":)", value)

    async def test_remove_interceptor(self):
        skip_if_client_version_older_than(self, "5.0")

        interceptor = MapGetInterceptor(":")
        registration_id = await self.map.add_interceptor(interceptor)
        self.assertIsNotNone(registration_id)
        self.assertTrue(await self.map.remove_interceptor(registration_id))

        # Unknown registration id should return False
        self.assertFalse(await self.map.remove_interceptor(registration_id))

        # Make sure that the interceptor is indeed removed
        await self.map.set(1, ")")
        value = await self.map.get(1)
        self.assertEqual(")", value)

    async def fill_map(self, count=10):
        m = {"key-%d" % x: "value-%d" % x for x in range(0, count)}
        await self.map.put_all(m)
        return m


class MapStoreTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        return config

    @classmethod
    def configure_cluster(cls):
        path = os.path.abspath(__file__)
        dir_path = os.path.dirname(path)
        with open(
            os.path.join(dir_path, "../../backward_compatible/proxy/hazelcast_mapstore.xml")
        ) as f:
            return f.read()

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.map = await self.client.get_map("mapstore-test")
        self.entries = await fill_map(self.map, size=10, key_prefix="key", value_prefix="val")

    async def asyncTearDown(self):
        await self.map.destroy()
        await super().asyncTearDown()

    async def test_load_all_with_no_args_loads_all_keys(self):
        await self.map.evict_all()
        await self.map.load_all()
        entry_set = await self.map.get_all(self.entries.keys())
        self.assertCountEqual(entry_set, self.entries)

    async def test_load_all_with_key_set_loads_given_keys(self):
        await self.map.evict_all()
        await self.map.load_all(["key0", "key1"])
        entry_set = await self.map.get_all(["key0", "key1"])
        self.assertCountEqual(entry_set, {"key0": "val0", "key1": "val1"})

    async def test_load_all_overrides_entries_in_memory_by_default(self):
        await self.map.evict_all()
        await self.map.put_transient("key0", "new0")
        await self.map.put_transient("key1", "new1")
        await self.map.load_all(["key0", "key1"])
        entry_set = await self.map.get_all(["key0", "key1"])
        self.assertCountEqual(entry_set, {"key0": "val0", "key1": "val1"})

    async def test_load_all_with_replace_existing_false_does_not_override(self):
        await self.map.evict_all()
        await self.map.put_transient("key0", "new0")
        await self.map.put_transient("key1", "new1")
        await self.map.load_all(["key0", "key1"], replace_existing_values=False)
        entry_set = await self.map.get_all(["key0", "key1"])
        self.assertCountEqual(entry_set, {"key0": "new0", "key1": "new1"})

    async def test_evict(self):
        await self.map.evict("key0")
        self.assertEqual(await self.map.size(), 9)

    async def test_evict_non_existing_key(self):
        await self.map.evict("non_existing_key")
        self.assertEqual(await self.map.size(), 10)

    async def test_evict_all(self):
        await self.map.evict_all()
        self.assertEqual(await self.map.size(), 0)

    async def test_add_entry_listener_item_loaded(self):
        collector = event_collector()
        await self.map.add_entry_listener(include_value=True, loaded_func=collector)
        await self.map.put("key", "value", ttl=0.1)
        await asyncio.sleep(2)
        await self.map.get("key")

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEntryEvent(event, key="key", value="value", event_type=EntryEventType.LOADED)

        await self.assertTrueEventually(assert_event, 10)


class MapTTLTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        return config

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.map = await self.client.get_map(random_string())

    async def asyncTearDown(self):
        await self.map.destroy()
        await super().asyncTearDown()

    async def test_put_default_ttl(self):
        await self.map.put("key", "value")
        await asyncio.sleep(1.0)
        self.assertTrue(await self.map.contains_key("key"))

    async def test_put(self):
        async def assert_map_not_contains():
            self.assertFalse(await self.map.contains_key("key"))

        await self.map.put("key", "value", 0.1)
        await self.assertTrueEventually(lambda: assert_map_not_contains())

    async def test_put_transient_default_ttl(self):
        await self.map.put_transient("key", "value")
        await asyncio.sleep(1.0)
        self.assertTrue(await self.map.contains_key("key"))

    async def test_put_transient(self):
        async def assert_map_not_contains():
            self.assertFalse(await self.map.contains_key("key"))

        await self.map.put_transient("key", "value", 0.1)
        await self.assertTrueEventually(lambda: assert_map_not_contains())

    async def test_put_if_absent_ttl(self):
        await self.map.put_if_absent("key", "value")
        await asyncio.sleep(1.0)
        self.assertTrue(await self.map.contains_key("key"))

    async def test_put_if_absent(self):
        async def assert_map_not_contains():
            self.assertFalse(await self.map.contains_key("key"))

        await self.map.put_if_absent("key", "value", 0.1)
        await self.assertTrueEventually(lambda: assert_map_not_contains())

    async def test_set_default_ttl(self):
        await self.map.set("key", "value")
        await asyncio.sleep(1.0)
        self.assertTrue(await self.map.contains_key("key"))

    async def test_set(self):
        async def assert_map_not_contains():
            self.assertFalse(await self.map.contains_key("key"))

        await self.map.set("key", "value", 0.1)
        await self.assertTrueEventually(lambda: assert_map_not_contains())


class MapMaxIdleTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        return config

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.map = await self.client.get_map(random_string())

    async def asyncTearDown(self):
        await self.map.destroy()
        await super().asyncTearDown()

    async def test_put_default_max_idle(self):
        await self.map.put("key", "value")
        await asyncio.sleep(1.0)
        self.assertTrue(await self.map.contains_key("key"))

    async def test_put(self):
        await self.map.put("key", "value", max_idle=0.1)
        await asyncio.sleep(1.0)
        self.assertFalse(await self.map.contains_key("key"))

    async def test_put_transient_default_max_idle(self):
        await self.map.put_transient("key", "value")
        await asyncio.sleep(1.0)
        self.assertTrue(await self.map.contains_key("key"))

    async def test_put_transient(self):
        await self.map.put_transient("key", "value", max_idle=0.1)
        await asyncio.sleep(1.0)
        self.assertFalse(await self.map.contains_key("key"))

    async def test_put_if_absent_max_idle(self):
        await self.map.put_if_absent("key", "value")
        await asyncio.sleep(1.0)
        self.assertTrue(await self.map.contains_key("key"))

    async def test_put_if_absent(self):
        await self.map.put_if_absent("key", "value", max_idle=0.1)
        await asyncio.sleep(1.0)
        self.assertFalse(await self.map.contains_key("key"))

    async def test_set_default_ttl(self):
        await self.map.set("key", "value")
        await asyncio.sleep(1.0)
        self.assertTrue(await self.map.contains_key("key"))

    async def test_set(self):
        await self.map.set("key", "value", max_idle=0.1)
        await asyncio.sleep(1.0)
        self.assertFalse(await self.map.contains_key("key"))


@unittest.skipIf(
    compare_client_version("4.2.1") < 0, "Tests the features added in 4.2.1 version of the client"
)
class MapAggregatorsIntTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        config["default_int_type"] = IntType.INT
        return config

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.map = await self.client.get_map(random_string())
        await self.map.put_all({"key-%d" % i: i for i in range(50)})

    async def asyncTearDown(self):
        await self.map.destroy()
        await super().asyncTearDown()

    async def test_aggregate_with_none_aggregator(self):
        with self.assertRaises(AssertionError):
            await self.map.aggregate(None)

    async def test_aggregate_with_paging_predicate(self):
        with self.assertRaises(AssertionError):
            await self.map.aggregate(int_avg("foo"), paging(true(), 10))

    async def test_int_average(self):
        average = await self.map.aggregate(int_avg())
        self.assertEqual(24.5, average)

    async def test_int_average_with_attribute_path(self):
        average = await self.map.aggregate(int_avg("this"))
        self.assertEqual(24.5, average)

    async def test_int_average_with_predicate(self):
        average = await self.map.aggregate(int_avg(), greater_or_equal("this", 47))
        self.assertEqual(48, average)

    async def test_int_sum(self):
        sum_ = await self.map.aggregate(int_sum())
        self.assertEqual(1225, sum_)

    async def test_int_sum_with_attribute_path(self):
        sum_ = await self.map.aggregate(int_sum("this"))
        self.assertEqual(1225, sum_)

    async def test_int_sum_with_predicate(self):
        sum_ = await self.map.aggregate(int_sum(), greater_or_equal("this", 47))
        self.assertEqual(144, sum_)

    async def test_fixed_point_sum(self):
        sum_ = await self.map.aggregate(fixed_point_sum())
        self.assertEqual(1225, sum_)

    async def test_fixed_point_sum_with_attribute_path(self):
        sum_ = await self.map.aggregate(fixed_point_sum("this"))
        self.assertEqual(1225, sum_)

    async def test_fixed_point_sum_with_predicate(self):
        sum_ = await self.map.aggregate(fixed_point_sum(), greater_or_equal("this", 47))
        self.assertEqual(144, sum_)

    async def test_distinct(self):
        await self._fill_with_duplicate_values()
        distinct_values = await self.map.aggregate(distinct())
        self.assertEqual(set(range(50)), distinct_values)

    async def test_distinct_with_attribute_path(self):
        await self._fill_with_duplicate_values()
        distinct_values = await self.map.aggregate(distinct("this"))
        self.assertEqual(set(range(50)), distinct_values)

    async def test_distinct_with_predicate(self):
        await self._fill_with_duplicate_values()
        distinct_values = await self.map.aggregate(distinct(), greater_or_equal("this", 10))
        self.assertEqual(set(range(10, 50)), distinct_values)

    async def test_max_by(self):
        max_item = await self.map.aggregate(max_by("this"))
        self.assertEqual("key-49", max_item.key)
        self.assertEqual(49, max_item.value)

    async def test_max_by_with_predicate(self):
        max_item = await self.map.aggregate(max_by("this"), less_or_equal("this", 10))
        self.assertEqual("key-10", max_item.key)
        self.assertEqual(10, max_item.value)

    async def test_min_by(self):
        min_item = await self.map.aggregate(min_by("this"))
        self.assertEqual("key-0", min_item.key)
        self.assertEqual(0, min_item.value)

    async def test_min_by_with_predicate(self):
        min_item = await self.map.aggregate(min_by("this"), greater_or_equal("this", 10))
        self.assertEqual("key-10", min_item.key)
        self.assertEqual(10, min_item.value)

    async def _fill_with_duplicate_values(self):
        # Map is initially filled with key-i: i mappings from [0, 50).
        # Add more values with different keys but the same values to
        # test the behaviour of the distinct aggregator.
        await self.map.put_all({"different-key-%d" % i: i for i in range(50)})


@unittest.skipIf(
    compare_client_version("4.2.1") < 0, "Tests the features added in 4.2.1 version of the client"
)
class MapAggregatorsLongTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        config["default_int_type"] = IntType.LONG
        return config

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.map = await self.client.get_map(random_string())
        await self.map.put_all({"key-%d" % i: i for i in range(50)})

    async def asyncTearDown(self):
        await self.map.destroy()
        await super().asyncTearDown()

    async def test_long_average(self):
        average = await self.map.aggregate(long_avg())
        self.assertEqual(24.5, average)

    async def test_long_average_with_attribute_path(self):
        average = await self.map.aggregate(long_avg("this"))
        self.assertEqual(24.5, average)

    async def test_long_average_with_predicate(self):
        average = await self.map.aggregate(long_avg(), greater_or_equal("this", 47))
        self.assertEqual(48, average)

    async def test_long_sum(self):
        sum_ = await self.map.aggregate(long_sum())
        self.assertEqual(1225, sum_)

    async def test_long_sum_with_attribute_path(self):
        sum_ = await self.map.aggregate(long_sum("this"))
        self.assertEqual(1225, sum_)

    async def test_long_sum_with_predicate(self):
        sum_ = await self.map.aggregate(long_sum(), greater_or_equal("this", 47))
        self.assertEqual(144, sum_)


@unittest.skipIf(
    compare_client_version("4.2.1") < 0, "Tests the features added in 4.2.1 version of the client"
)
class MapAggregatorsDoubleTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        return config

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.map = await self.client.get_map(random_string())
        await self.map.put_all({"key-%d" % i: float(i) for i in range(50)})

    async def asyncTearDown(self):
        await self.map.destroy()
        await super().asyncTearDown()

    async def test_count(self):
        count_ = await self.map.aggregate(count())
        self.assertEqual(50, count_)

    async def test_count_with_attribute_path(self):
        count_ = await self.map.aggregate(count("this"))
        self.assertEqual(50, count_)

    async def test_count_with_predicate(self):
        count_ = await self.map.aggregate(count(), greater_or_equal("this", 1))
        self.assertEqual(49, count_)

    async def test_double_average(self):
        average = await self.map.aggregate(double_avg())
        self.assertEqual(24.5, average)

    async def test_double_average_with_attribute_path(self):
        average = await self.map.aggregate(double_avg("this"))
        self.assertEqual(24.5, average)

    async def test_double_average_with_predicate(self):
        average = await self.map.aggregate(double_avg(), greater_or_equal("this", 47))
        self.assertEqual(48, average)

    async def test_double_sum(self):
        sum_ = await self.map.aggregate(double_sum())
        self.assertEqual(1225, sum_)

    async def test_double_sum_with_attribute_path(self):
        sum_ = await self.map.aggregate(double_sum("this"))
        self.assertEqual(1225, sum_)

    async def test_double_sum_with_predicate(self):
        sum_ = await self.map.aggregate(double_sum(), greater_or_equal("this", 47))
        self.assertEqual(144, sum_)

    async def test_floating_point_sum(self):
        sum_ = await self.map.aggregate(floating_point_sum())
        self.assertEqual(1225, sum_)

    async def test_floating_point_sum_with_attribute_path(self):
        sum_ = await self.map.aggregate(floating_point_sum("this"))
        self.assertEqual(1225, sum_)

    async def test_floating_point_sum_with_predicate(self):
        sum_ = await self.map.aggregate(floating_point_sum(), greater_or_equal("this", 47))
        self.assertEqual(144, sum_)

    async def test_number_avg(self):
        average = await self.map.aggregate(number_avg())
        self.assertEqual(24.5, average)

    async def test_number_avg_with_attribute_path(self):
        average = await self.map.aggregate(number_avg("this"))
        self.assertEqual(24.5, average)

    async def test_number_avg_with_predicate(self):
        average = await self.map.aggregate(number_avg(), greater_or_equal("this", 47))
        self.assertEqual(48, average)

    async def test_max(self):
        average = await self.map.aggregate(max_())
        self.assertEqual(49, average)

    async def test_max_with_attribute_path(self):
        average = await self.map.aggregate(max_("this"))
        self.assertEqual(49, average)

    async def test_max_with_predicate(self):
        average = await self.map.aggregate(max_(), less_or_equal("this", 3))
        self.assertEqual(3, average)

    async def test_min(self):
        average = await self.map.aggregate(min_())
        self.assertEqual(0, average)

    async def test_min_with_attribute_path(self):
        average = await self.map.aggregate(min_("this"))
        self.assertEqual(0, average)

    async def test_min_with_predicate(self):
        average = await self.map.aggregate(min_(), greater_or_equal("this", 3))
        self.assertEqual(3, average)


@unittest.skipIf(
    compare_client_version("4.2.1") < 0, "Tests the features added in 4.2.1 version of the client"
)
class MapProjectionsTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        return config

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.map = await self.client.get_map(random_string())
        await self.map.put(1, HazelcastJsonValue('{"attr1": 1, "attr2": 2, "attr3": 3}'))
        await self.map.put(2, HazelcastJsonValue('{"attr1": 4, "attr2": 5, "attr3": 6}'))

    async def asyncTearDown(self):
        await self.map.destroy()
        await super().asyncTearDown()

    async def test_project_with_none_projection(self):
        with self.assertRaises(AssertionError):
            await self.map.project(None)

    async def test_project_with_paging_predicate(self):
        with self.assertRaises(AssertionError):
            await self.map.project(single_attribute("foo"), paging(true(), 10))

    async def test_single_attribute(self):
        attributes = await self.map.project(single_attribute("attr1"))
        self.assertCountEqual([1, 4], attributes)

    async def test_single_attribute_with_predicate(self):
        attributes = await self.map.project(single_attribute("attr1"), greater_or_equal("attr1", 4))
        self.assertCountEqual([4], attributes)

    async def test_multi_attribute(self):
        attributes = await self.map.project(multi_attribute("attr1", "attr2"))
        self.assertCountEqual([[1, 2], [4, 5]], attributes)

    async def test_multi_attribute_with_predicate(self):
        attributes = await self.map.project(
            multi_attribute("attr1", "attr2"),
            greater_or_equal("attr2", 3),
        )
        self.assertCountEqual([[4, 5]], attributes)

    async def test_identity(self):
        attributes = await self.map.project(identity())
        self.assertCountEqual(
            [
                HazelcastJsonValue('{"attr1": 4, "attr2": 5, "attr3": 6}'),
                HazelcastJsonValue('{"attr1": 1, "attr2": 2, "attr3": 3}'),
            ],
            [attribute.value for attribute in attributes],
        )

    async def test_identity_with_predicate(self):
        attributes = await self.map.project(identity(), greater_or_equal("attr2", 3))
        self.assertCountEqual(
            [HazelcastJsonValue('{"attr1": 4, "attr2": 5, "attr3": 6}')],
            [attribute.value for attribute in attributes],
        )
