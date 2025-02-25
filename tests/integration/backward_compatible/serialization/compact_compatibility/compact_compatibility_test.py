import copy
import enum
import typing
import unittest

import pytest

from hazelcast.errors import NullPointerError, IllegalMonitorStateError
from hazelcast.predicate import Predicate, paging
from tests.base import HazelcastTestCase
from tests.util import (
    random_string,
    compare_client_version,
    compare_server_version_with_rc,
    skip_if_client_version_older_than,
    skip_if_server_version_older_than,
    skip_if_server_version_newer_than_or_equal,
)

try:
    from hazelcast.serialization.api import (
        CompactSerializer,
        CompactWriter,
        CompactReader,
    )
except ImportError:
    # For backward compatibility tests

    T = typing.TypeVar("T")

    class CompactSerializer(typing.Generic[T]):
        pass

    class CompactReader:
        pass

    class CompactWriter:
        pass

    class FieldKind(enum.Enum):
        pass


class InnerCompact:
    def __init__(self, string_field: str):
        self.string_field = string_field

    def __eq__(self, o: object) -> bool:
        return isinstance(o, InnerCompact) and self.string_field == o.string_field

    def __hash__(self) -> int:
        return hash(self.string_field)

    def __repr__(self):
        return f"InnerCompact(string_field={self.string_field})"


class OuterCompact:
    def __init__(self, int_field: int, inner_field: InnerCompact):
        self.int_field = int_field
        self.inner_field = inner_field

    def __eq__(self, o: object) -> bool:
        return (
            isinstance(o, OuterCompact)
            and self.int_field == o.int_field
            and self.inner_field == o.inner_field
        )

    def __hash__(self) -> int:
        return hash((self.int_field, self.inner_field))

    def __repr__(self):
        return f"OuterCompact(int_field={self.int_field}, inner_field={self.inner_field})"


class InnerSerializer(CompactSerializer[InnerCompact]):
    def read(self, reader: CompactReader) -> InnerCompact:
        return InnerCompact(reader.read_string("stringField"))

    def write(self, writer: CompactWriter, obj: InnerCompact) -> None:
        writer.write_string("stringField", obj.string_field)

    def get_type_name(self) -> str:
        return "com.hazelcast.serialization.compact.InnerCompact"

    def get_class(self):
        return InnerCompact


class OuterSerializer(CompactSerializer[OuterCompact]):
    def read(self, reader: CompactReader) -> OuterCompact:
        return OuterCompact(
            reader.read_int32("intField"),
            reader.read_compact("innerField"),
        )

    def write(self, writer: CompactWriter, obj: OuterCompact) -> None:
        writer.write_int32("intField", obj.int_field)
        writer.write_compact("innerField", obj.inner_field)

    def get_type_name(self) -> str:
        return "com.hazelcast.serialization.compact.OuterCompact"

    def get_class(self):
        return OuterCompact


class CompactIncrementFunction:
    pass


class CompactIncrementFunctionSerializer(CompactSerializer[CompactIncrementFunction]):
    def read(self, reader: CompactReader) -> CompactIncrementFunction:
        return CompactIncrementFunction()

    def write(self, writer: CompactWriter, obj: CompactIncrementFunction) -> None:
        pass

    def get_type_name(self) -> str:
        return "com.hazelcast.serialization.compact.CompactIncrementFunction"

    def get_class(self):
        return CompactIncrementFunction


class CompactReturningFunction:
    pass


class CompactReturningFunctionSerializer(CompactSerializer[CompactReturningFunction]):
    def read(self, reader: CompactReader) -> CompactReturningFunction:
        return CompactReturningFunction()

    def write(self, writer: CompactWriter, obj: CompactReturningFunction) -> None:
        pass

    def get_type_name(self) -> str:
        return "com.hazelcast.serialization.compact.CompactReturningFunction"

    def get_class(self):
        return CompactReturningFunction


class CompactReturningCallable:
    pass


class CompactReturningCallableSerializer(CompactSerializer[CompactReturningCallable]):
    def read(self, reader: CompactReader) -> CompactReturningCallable:
        return CompactReturningCallable()

    def write(self, writer: CompactWriter, obj: CompactReturningCallable) -> None:
        pass

    def get_type_name(self) -> str:
        return "com.hazelcast.serialization.compact.CompactReturningCallable"

    def get_class(self):
        return CompactReturningCallable


class CompactPredicate(Predicate):
    pass


class CompactPredicateSerializer(CompactSerializer[CompactPredicate]):
    def read(self, reader: CompactReader) -> CompactPredicate:
        return CompactPredicate()

    def write(self, writer: CompactWriter, obj: CompactPredicate) -> None:
        pass

    def get_type_name(self) -> str:
        return "com.hazelcast.serialization.compact.CompactPredicate"

    def get_class(self):
        return CompactPredicate


class CompactReturningMapInterceptor:
    pass


class CompactReturningMapInterceptorSerializer(CompactSerializer[CompactReturningMapInterceptor]):
    def read(self, reader: CompactReader) -> CompactReturningMapInterceptor:
        return CompactReturningMapInterceptor()

    def write(self, writer: CompactWriter, obj: CompactReturningMapInterceptor) -> None:
        pass

    def get_type_name(self) -> str:
        return "com.hazelcast.serialization.compact.CompactReturningMapInterceptor"

    def get_class(self):
        return CompactReturningMapInterceptor


try:
    from hazelcast.aggregator import Aggregator

    class CompactReturningAggregator(Aggregator):
        pass

except ImportError:

    class CompactReturningAggregator:
        pass


class CompactReturningAggregatorSerializer(CompactSerializer[CompactReturningAggregator]):
    def read(self, reader: CompactReader) -> CompactReturningAggregator:
        return CompactReturningAggregator()

    def write(self, writer: CompactWriter, obj: CompactReturningAggregator) -> None:
        pass

    def get_type_name(self) -> str:
        return "com.hazelcast.serialization.compact.CompactReturningAggregator"

    def get_class(self):
        return CompactReturningAggregator


class CompactReturningEntryProcessor:
    pass


class CompactReturningEntryProcessorSerializer(CompactSerializer[CompactReturningEntryProcessor]):
    def read(self, reader: CompactReader) -> CompactReturningEntryProcessor:
        return CompactReturningEntryProcessor()

    def write(self, writer: CompactWriter, obj: CompactReturningEntryProcessor) -> None:
        pass

    def get_type_name(self) -> str:
        return "com.hazelcast.serialization.compact.CompactReturningEntryProcessor"

    def get_class(self):
        return CompactReturningEntryProcessor


class CompactReturningProjection:
    pass


class CompactReturningProjectionSerializer(CompactSerializer[CompactReturningProjection]):
    def read(self, reader: CompactReader) -> CompactReturningProjection:
        return CompactReturningProjection()

    def write(self, writer: CompactWriter, obj: CompactReturningProjection) -> None:
        pass

    def get_type_name(self) -> str:
        return "com.hazelcast.serialization.compact.CompactReturningProjection"

    def get_class(self):
        return CompactReturningProjection


class CompactFilter:
    pass


class CompactFilterSerializer(CompactSerializer[CompactFilter]):
    def read(self, reader: CompactReader) -> CompactFilter:
        return CompactFilter()

    def write(self, writer: CompactWriter, obj: CompactFilter) -> None:
        pass

    def get_type_name(self) -> str:
        return "com.hazelcast.serialization.compact.CompactFilter"

    def get_class(self):
        return CompactFilter


INNER_COMPACT_INSTANCE = InnerCompact("42")
OUTER_COMPACT_INSTANCE = OuterCompact(42, INNER_COMPACT_INSTANCE)


@unittest.skipIf(
    compare_client_version("5.2") < 0, "Tests the features added in 5.2 version of the client"
)
class CompactCompatibilityBase(HazelcastTestCase):
    rc = None
    cluster = None
    client_config = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.rc = cls.create_rc()
        if compare_server_version_with_rc(cls.rc, "5.2") < 0:
            cls.rc.exit()
            raise unittest.SkipTest("Compact serialization requires 5.2 server")

        config = f"""
<hazelcast xmlns="http://www.hazelcast.com/schema/config"
           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
           xsi:schemaLocation="http://www.hazelcast.com/schema/config
           http://www.hazelcast.com/schema/config/hazelcast-config-5.2.xsd">
    <jet enabled="true" />
</hazelcast>
        """

        cls.cluster = cls.create_cluster(cls.rc, config)
        cls.cluster.start_member()
        cls.client_config = {
            "cluster_name": cls.cluster.id,
            "compact_serializers": [
                InnerSerializer(),
                OuterSerializer(),
                CompactIncrementFunctionSerializer(),
                CompactReturningFunctionSerializer(),
                CompactReturningCallableSerializer(),
                CompactPredicateSerializer(),
                CompactReturningMapInterceptorSerializer(),
                CompactReturningAggregatorSerializer(),
                CompactReturningEntryProcessorSerializer(),
                CompactReturningProjectionSerializer(),
                CompactFilterSerializer(),
            ],
        }

    @classmethod
    def tearDownClass(cls) -> None:
        cls.rc.terminateCluster(cls.cluster.id)
        cls.rc.exit()

    def setUp(self) -> None:
        self.client = self.create_client(self.client_config)

    def tearDown(self) -> None:
        self.shutdown_all_clients()


@pytest.mark.enterprise
class AtomicLongCompactCompatibilityTest(CompactCompatibilityBase):
    def setUp(self) -> None:
        super().setUp()
        self.atomic_long = self.client.cp_subsystem.get_atomic_long(random_string()).blocking()
        self.atomic_long.set(41)

    def tearDown(self) -> None:
        self.atomic_long.destroy()
        super().tearDown()

    def test_alter(self):
        self.atomic_long.alter(CompactIncrementFunction())
        self.assertEqual(42, self.atomic_long.get())

    def test_alter_and_get(self):
        self.assertEqual(42, self.atomic_long.alter_and_get(CompactIncrementFunction()))

    def test_get_and_alter(self):
        self.assertEqual(41, self.atomic_long.get_and_alter(CompactIncrementFunction()))
        self.assertEqual(42, self.atomic_long.get())

    def test_apply(self):
        self.assertEqual(OUTER_COMPACT_INSTANCE, self.atomic_long.apply(CompactReturningFunction()))


@pytest.mark.enterprise
class AtomicReferenceCompactCompatibilityTest(CompactCompatibilityBase):
    def setUp(self) -> None:
        super().setUp()
        self.atomic_reference = self.client.cp_subsystem.get_atomic_reference(
            random_string()
        ).blocking()
        self.atomic_reference.set(None)

    def tearDown(self) -> None:
        self.atomic_reference.destroy()
        super().tearDown()

    def test_compare_and_set(self):
        self.assertTrue(self.atomic_reference.compare_and_set(None, OUTER_COMPACT_INSTANCE))
        self.assertEqual(OUTER_COMPACT_INSTANCE, self.atomic_reference.get())

    def test_set(self):
        self.atomic_reference.set(OUTER_COMPACT_INSTANCE)
        self.assertEqual(OUTER_COMPACT_INSTANCE, self.atomic_reference.get())

    def test_get_and_set(self):
        self.assertEqual(None, self.atomic_reference.get_and_set(OUTER_COMPACT_INSTANCE))
        self.assertEqual(OUTER_COMPACT_INSTANCE, self.atomic_reference.get_and_set(None))

    def test_contains(self):
        self.assertFalse(self.atomic_reference.contains(OUTER_COMPACT_INSTANCE))
        self.atomic_reference.set(OUTER_COMPACT_INSTANCE)
        self.assertTrue(self.atomic_reference.contains(OUTER_COMPACT_INSTANCE))

    def test_alter(self):
        self.atomic_reference.alter(CompactReturningFunction())
        self.assertEqual(OUTER_COMPACT_INSTANCE, self.atomic_reference.get())

    def test_alter_and_get(self):
        self.assertEqual(
            OUTER_COMPACT_INSTANCE,
            self.atomic_reference.alter_and_get(CompactReturningFunction()),
        )

    def test_get_and_alter(self):
        self.assertEqual(None, self.atomic_reference.get_and_alter(CompactReturningFunction()))
        self.assertEqual(
            OUTER_COMPACT_INSTANCE,
            self.atomic_reference.get_and_alter(CompactReturningFunction()),
        )

    def test_apply(self):
        self.assertEqual(
            OUTER_COMPACT_INSTANCE,
            self.atomic_reference.apply(CompactReturningFunction()),
        )

    def test_get(self):
        self.atomic_reference.set(OUTER_COMPACT_INSTANCE)
        self.assertEqual(OUTER_COMPACT_INSTANCE, self.atomic_reference.get())


class ExecutorCompactCompatibilityTest(CompactCompatibilityBase):
    def setUp(self) -> None:
        super().setUp()
        self.executor = self.client.get_executor(random_string()).blocking()

    def tearDown(self) -> None:
        self.executor.destroy()
        super().tearDown()

    def test_execute_on_key_owner(self):
        self.assertEqual(
            OUTER_COMPACT_INSTANCE,
            self.executor.execute_on_key_owner(OUTER_COMPACT_INSTANCE, CompactReturningCallable()),
        )

    def test_execute_on_member(self):
        member = self.client.cluster_service.get_members()[0]
        self.assertEqual(
            OUTER_COMPACT_INSTANCE,
            self.executor.execute_on_member(member, CompactReturningCallable()),
        )

    def test_execute_on_members(self):
        member = self.client.cluster_service.get_members()[0]
        self.assertEqual(
            OUTER_COMPACT_INSTANCE,
            self.executor.execute_on_members([member], CompactReturningCallable())[0],
        )

    def test_execute_on_all_members(self):
        self.assertEqual(
            OUTER_COMPACT_INSTANCE,
            self.executor.execute_on_all_members(CompactReturningCallable())[0],
        )


class ListCompactCompatibilityTest(CompactCompatibilityBase):
    def setUp(self) -> None:
        super().setUp()
        self.list = self.client.get_list(random_string()).blocking()

    def tearDown(self) -> None:
        self.list.destroy()
        super().tearDown()

    def test_add(self):
        self.assertTrue(self.list.add(OUTER_COMPACT_INSTANCE))
        self.assertEqual(OUTER_COMPACT_INSTANCE, self.list.get(0))

    def test_add_at(self):
        self.list.add_at(0, OUTER_COMPACT_INSTANCE)
        self.assertEqual(OUTER_COMPACT_INSTANCE, self.list.get(0))

    def test_add_all(self):
        self.assertTrue(self.list.add_all([INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE]))
        self.assertEqual(INNER_COMPACT_INSTANCE, self.list.get(0))
        self.assertEqual(OUTER_COMPACT_INSTANCE, self.list.get(1))

    def test_add_all_at(self):
        self.assertTrue(self.list.add_all_at(0, [INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE]))
        self.assertEqual(INNER_COMPACT_INSTANCE, self.list.get(0))
        self.assertEqual(OUTER_COMPACT_INSTANCE, self.list.get(1))

    def test_add_listener(self):
        events = []

        def listener(event):
            events.append(event)

        self.list.add_listener(include_value=True, item_added_func=listener)

        self._add_from_another_client(OUTER_COMPACT_INSTANCE)

        def assertion():
            self.assertEqual(1, len(events))
            event = events[0]
            self.assertEqual(OUTER_COMPACT_INSTANCE, event.item)

        self.assertTrueEventually(assertion)

    def test_contains(self):
        self.assertFalse(self.list.contains(OUTER_COMPACT_INSTANCE))
        self.list.add(OUTER_COMPACT_INSTANCE)
        self.assertTrue(self.list.contains(OUTER_COMPACT_INSTANCE))

    def test_contains_all(self):
        items = [INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE]
        self.assertFalse(self.list.contains_all(items))
        self.list.add_all(items)
        self.assertTrue(self.list.contains_all(items))

    def test_get(self):
        self._add_from_another_client(OUTER_COMPACT_INSTANCE)
        self.assertEqual(OUTER_COMPACT_INSTANCE, self.list.get(0))

    def test_get_all(self):
        self._add_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual([INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE], self.list.get_all())

    def test_index_of(self):
        self.assertEqual(-1, self.list.index_of(OUTER_COMPACT_INSTANCE))
        self.list.add(OUTER_COMPACT_INSTANCE)
        self.assertEqual(0, self.list.index_of(OUTER_COMPACT_INSTANCE))

    def test_iterator(self):
        self._add_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual([INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE], self.list.iterator())

    def test_last_index_of(self):
        self.assertEqual(-1, self.list.last_index_of(OUTER_COMPACT_INSTANCE))
        self.list.add(OUTER_COMPACT_INSTANCE)
        self.assertEqual(0, self.list.last_index_of(OUTER_COMPACT_INSTANCE))

    def test_list_iterator(self):
        self._add_from_another_client(
            INNER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE
        )
        self.assertEqual(
            [INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE], self.list.list_iterator(1)
        )

    def test_remove(self):
        self.assertFalse(self.list.remove(OUTER_COMPACT_INSTANCE))
        self.list.add(OUTER_COMPACT_INSTANCE)
        self.assertTrue(self.list.remove(OUTER_COMPACT_INSTANCE))

    def test_remove_at(self):
        self._add_from_another_client(OUTER_COMPACT_INSTANCE)
        self.assertEqual(OUTER_COMPACT_INSTANCE, self.list.remove_at(0))

    def test_remove_all(self):
        items = [INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE]
        self.assertFalse(self.list.remove_all(items))
        self.list.add_all(items)
        self.assertTrue(self.list.remove_all(items))

    def test_retain_all(self):
        items = [INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE]
        self.assertFalse(self.list.retain_all(items))
        self.list.add_all(items + ["x"])
        self.assertTrue(self.list.retain_all(items))

    def test_set_at(self):
        self.list.add(42)
        self.assertEqual(42, self.list.set_at(0, OUTER_COMPACT_INSTANCE))
        self.assertEqual(OUTER_COMPACT_INSTANCE, self.list.set_at(0, INNER_COMPACT_INSTANCE))

    def test_sublist(self):
        self._add_from_another_client(
            INNER_COMPACT_INSTANCE,
            INNER_COMPACT_INSTANCE,
            OUTER_COMPACT_INSTANCE,
            OUTER_COMPACT_INSTANCE,
        )
        self.assertEqual([INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE], self.list.sub_list(1, 3))

    def _add_from_another_client(self, *values):
        other_client = self.create_client(self.client_config)
        other_client_list = other_client.get_list(self.list.name).blocking()
        other_client_list.add_all(values)


class MapCompatibilityTest(CompactCompatibilityBase):
    def setUp(self) -> None:
        super().setUp()
        self.map = self.client.get_map(random_string()).blocking()

    def tearDown(self) -> None:
        self.map.destroy()
        super().tearDown()

    def test_add_entry_listener_with_key_and_predicate(self):
        events = []

        def listener(event):
            events.append(event)

        self.map.add_entry_listener(
            include_value=True,
            key=INNER_COMPACT_INSTANCE,
            predicate=CompactPredicate(),
            added_func=listener,
        )

        self._assert_entry_event(events)

    def test_add_entry_listener_with_predicate(self):
        events = []

        def listener(event):
            events.append(event)

        self.map.add_entry_listener(
            include_value=True,
            predicate=CompactPredicate(),
            added_func=listener,
        )

        self._assert_entry_event(events)

    def test_add_entry_listener_with_key(self):
        events = []

        def listener(event):
            events.append(event)

        self.map.add_entry_listener(
            include_value=True,
            key=INNER_COMPACT_INSTANCE,
            added_func=listener,
        )

        self._assert_entry_event(events)

    def test_add_interceptor(self):
        self.map.add_interceptor(CompactReturningMapInterceptor())
        self.assertEqual(OUTER_COMPACT_INSTANCE, self.map.get("non-existent-key"))

    def test_aggregate(self):
        self.assertEqual(
            OUTER_COMPACT_INSTANCE,
            self.map.aggregate(CompactReturningAggregator()),
        )

    def test_aggregate_with_predicate(self):
        self.assertEqual(
            OUTER_COMPACT_INSTANCE,
            self.map.aggregate(CompactReturningAggregator(), predicate=CompactPredicate()),
        )

    def test_contains_key(self):
        self.assertFalse(self.map.contains_key(OUTER_COMPACT_INSTANCE))
        self.map.put(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        self.assertTrue(self.map.contains_key(OUTER_COMPACT_INSTANCE))

    def test_contains_value(self):
        self.assertFalse(self.map.contains_value(OUTER_COMPACT_INSTANCE))
        self.map.put(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertTrue(self.map.contains_value(OUTER_COMPACT_INSTANCE))

    def test_delete(self):
        self.map.delete(OUTER_COMPACT_INSTANCE)
        self.map.put(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        self.map.delete(OUTER_COMPACT_INSTANCE)
        self.assertIsNone(self.map.get(OUTER_COMPACT_INSTANCE))

    def test_entry_set(self):
        self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual([(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)], self.map.entry_set())

    def test_entry_set_with_predicate(self):
        self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual(
            [(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)],
            self.map.entry_set(CompactPredicate()),
        )

    def test_entry_set_with_paging_predicate(self):
        # https://github.com/hazelcast/hazelcast-python-client/issues/666
        skip_if_server_version_newer_than_or_equal(self, self.client, "5.4")
        self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual(
            [(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)],
            self.map.entry_set(paging(CompactPredicate(), 1)),
        )

    def test_evict(self):
        self.assertFalse(self.map.evict(OUTER_COMPACT_INSTANCE))
        self.map.put(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        self.assertTrue(self.map.evict(OUTER_COMPACT_INSTANCE))

    def test_execute_on_entries(self):
        self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual(
            [(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)],
            self.map.execute_on_entries(CompactReturningEntryProcessor()),
        )

    def test_execute_on_entries_predicate(self):
        self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual(
            [(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)],
            self.map.execute_on_entries(CompactReturningEntryProcessor(), CompactPredicate()),
        )

    def test_execute_on_key(self):
        self.map.put(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        self.assertEqual(
            OUTER_COMPACT_INSTANCE,
            self.map.execute_on_key(OUTER_COMPACT_INSTANCE, CompactReturningEntryProcessor()),
        )

    def test_execute_on_keys(self):
        self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual(
            [(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)],
            self.map.execute_on_keys([INNER_COMPACT_INSTANCE], CompactReturningEntryProcessor()),
        )

    def test_force_unlock(self):
        self.map.force_unlock(OUTER_COMPACT_INSTANCE)
        self.map.lock(OUTER_COMPACT_INSTANCE)
        self.map.force_unlock(OUTER_COMPACT_INSTANCE)
        self.assertFalse(self.map.is_locked(OUTER_COMPACT_INSTANCE))

    def test_get(self):
        self._put_from_another_client(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        self.assertEqual(INNER_COMPACT_INSTANCE, self.map.get(OUTER_COMPACT_INSTANCE))

    def test_get_all(self):
        self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual(
            {INNER_COMPACT_INSTANCE: OUTER_COMPACT_INSTANCE},
            self.map.get_all([INNER_COMPACT_INSTANCE]),
        )

    def test_get_entry_view(self):
        self._put_from_another_client(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        entry_view = self.map.get_entry_view(OUTER_COMPACT_INSTANCE)
        self.assertEqual(OUTER_COMPACT_INSTANCE, entry_view.key)
        self.assertEqual(INNER_COMPACT_INSTANCE, entry_view.value)

    def test_is_locked(self):
        self.assertFalse(self.map.is_locked(OUTER_COMPACT_INSTANCE))
        self.map.lock(OUTER_COMPACT_INSTANCE)
        self.assertTrue(self.map.is_locked(OUTER_COMPACT_INSTANCE))

    def test_key_set(self):
        self._put_from_another_client(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        self.assertEqual([OUTER_COMPACT_INSTANCE], self.map.key_set())

    def test_key_set_with_predicate(self):
        self._put_from_another_client(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        self.assertEqual(
            [OUTER_COMPACT_INSTANCE],
            self.map.key_set(CompactPredicate()),
        )

    def test_key_set_with_paging_predicate(self):
        # https://github.com/hazelcast/hazelcast-python-client/issues/666
        skip_if_server_version_newer_than_or_equal(self, self.client, "5.4")
        self._put_from_another_client(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        self.assertEqual(
            [OUTER_COMPACT_INSTANCE],
            self.map.key_set(paging(CompactPredicate(), 1)),
        )

    def test_load_all_with_keys(self):
        try:
            self.map.load_all(keys=[INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE])
        except NullPointerError:
            # Since there is no loader configured
            # the server throws this error. In this test,
            # we only care about sending the serialized
            # for of the keys to the server. So, we don't
            # care about what server does with these keys.
            # It should probably handle this gracefully,
            # but it is OK.
            pass

    def test_lock(self):
        self.map.lock(OUTER_COMPACT_INSTANCE)
        self.assertTrue(self.map.is_locked(OUTER_COMPACT_INSTANCE))

    def test_project(self):
        self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual(
            [OUTER_COMPACT_INSTANCE],
            self.map.project(CompactReturningProjection()),
        )

    def test_project_with_predicate(self):
        self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual(
            [OUTER_COMPACT_INSTANCE],
            self.map.project(CompactReturningProjection(), CompactPredicate()),
        )

    def test_put(self):
        self.map.put(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual(OUTER_COMPACT_INSTANCE, self.map.get(INNER_COMPACT_INSTANCE))

    def test_put_all(self):
        self.map.put_all({OUTER_COMPACT_INSTANCE: INNER_COMPACT_INSTANCE})
        self.assertEqual(INNER_COMPACT_INSTANCE, self.map.get(OUTER_COMPACT_INSTANCE))

    def test_put_if_absent(self):
        self.assertIsNone(self.map.put_if_absent(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE))
        self.assertEqual(
            OUTER_COMPACT_INSTANCE,
            self.map.put_if_absent(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE),
        )

    def test_put_transient(self):
        self.map.put_transient(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual(OUTER_COMPACT_INSTANCE, self.map.get(INNER_COMPACT_INSTANCE))

    def test_remove(self):
        self.assertIsNone(self.map.remove(OUTER_COMPACT_INSTANCE))
        self._put_from_another_client(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        self.assertEqual(INNER_COMPACT_INSTANCE, self.map.remove(OUTER_COMPACT_INSTANCE))

    def test_remove_all(self):
        skip_if_client_version_older_than(self, "5.2")

        self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertIsNone(self.map.remove_all(CompactPredicate()))
        self.assertEqual(0, self.map.size())

    def test_remove_if_same(self):
        self.assertFalse(self.map.remove_if_same(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE))
        self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertTrue(self.map.remove_if_same(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE))

    def test_replace(self):
        self.assertIsNone(self.map.replace(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE))
        self.map.put(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual(
            OUTER_COMPACT_INSTANCE, self.map.replace(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        )

    def test_replace_if_same(self):
        self.assertFalse(
            self.map.replace_if_same(
                INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE
            )
        )
        self.map.put(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertTrue(
            self.map.replace_if_same(
                INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE
            )
        )

    def test_set(self):
        self.map.set(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual(OUTER_COMPACT_INSTANCE, self.map.get(INNER_COMPACT_INSTANCE))

    def test_set_ttl(self):
        self._put_from_another_client(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        self.map.set_ttl(OUTER_COMPACT_INSTANCE, 999)

    def test_try_lock(self):
        self.assertTrue(self.map.try_lock(OUTER_COMPACT_INSTANCE))

    def test_try_put(self):
        self.assertTrue(self.map.try_put(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE))

    def test_try_remove(self):
        self.assertFalse(self.map.try_remove(OUTER_COMPACT_INSTANCE))
        self.map.put(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        self.assertTrue(self.map.try_remove(OUTER_COMPACT_INSTANCE))

    def test_unlock(self):
        try:
            self.map.unlock(OUTER_COMPACT_INSTANCE)
        except IllegalMonitorStateError:
            # Lock is not locked, but we don't care
            # about it, as we only want to verify
            # that we can send the serialized form
            # of key to the server.
            pass

    def test_values(self):
        self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual([OUTER_COMPACT_INSTANCE], self.map.values())

    def test_values_with_predicate(self):
        self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual([OUTER_COMPACT_INSTANCE], self.map.values(CompactPredicate()))

    def test_values_with_paging_predicate(self):
        # https://github.com/hazelcast/hazelcast-python-client/issues/666
        skip_if_server_version_newer_than_or_equal(self, self.client, "5.4")
        self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual([OUTER_COMPACT_INSTANCE], self.map.values(paging(CompactPredicate(), 1)))

    def _put_from_another_client(self, key, value):
        other_client = self.create_client(self.client_config)
        other_client_map = other_client.get_map(self.map.name).blocking()
        other_client_map.put(key, value)

    def _assert_entry_event(self, events):
        self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)

        def assertion():
            self.assertEqual(1, len(events))
            event = events[0]
            self.assertEqual(INNER_COMPACT_INSTANCE, event.key)
            self.assertEqual(OUTER_COMPACT_INSTANCE, event.value)
            self.assertIsNone(event.old_value)
            self.assertIsNone(event.merging_value)

        self.assertTrueEventually(assertion)


class NearCachedMapCompactCompatibilityTest(MapCompatibilityTest):
    def setUp(self) -> None:
        map_name = random_string()
        self.client_config = copy.deepcopy(NearCachedMapCompactCompatibilityTest.client_config)
        self.client_config["near_caches"] = {map_name: {}}
        super().setUp()
        self.map = self.client.get_map(map_name).blocking()

    def test_get_for_near_cache(self):
        # Another variant of the test in the superclass, where we lookup a key
        # which has a value whose schema is not fully sent to the
        # cluster from this client. The near cache will try to serialize it,
        # but it should not attempt to send this schema to the cluster, as it
        # is just fetched from there.
        self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual(OUTER_COMPACT_INSTANCE, self.map.get(INNER_COMPACT_INSTANCE))


class MultiMapCompactCompatibilityTest(CompactCompatibilityBase):
    def setUp(self) -> None:
        super().setUp()
        self.multi_map = self.client.get_multi_map(random_string()).blocking()

    def tearDown(self) -> None:
        self.multi_map.destroy()
        super().tearDown()

    def test_add_entry_listener(self):
        events = []

        def listener(event):
            events.append(event)

        self.multi_map.add_entry_listener(
            include_value=True,
            added_func=listener,
        )

        self._assert_entry_event(events)

    def test_add_entry_listener_with_key(self):
        events = []

        def listener(event):
            events.append(event)

        self.multi_map.add_entry_listener(
            include_value=True,
            key=INNER_COMPACT_INSTANCE,
            added_func=listener,
        )

        self._assert_entry_event(events)

    def test_contains_key(self):
        self.assertFalse(self.multi_map.contains_key(OUTER_COMPACT_INSTANCE))
        self.multi_map.put(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        self.assertTrue(self.multi_map.contains_key(OUTER_COMPACT_INSTANCE))

    def test_contains_value(self):
        self.assertFalse(self.multi_map.contains_value(OUTER_COMPACT_INSTANCE))
        self.multi_map.put(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertTrue(self.multi_map.contains_value(OUTER_COMPACT_INSTANCE))

    def test_contains_entry(self):
        self.assertFalse(
            self.multi_map.contains_entry(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        )
        self.multi_map.put(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertTrue(
            self.multi_map.contains_entry(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        )

    def test_entry_set(self):
        self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual(
            [(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)], self.multi_map.entry_set()
        )

    def test_get(self):
        self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual([OUTER_COMPACT_INSTANCE], self.multi_map.get(INNER_COMPACT_INSTANCE))

    def test_is_locked(self):
        self.assertFalse(self.multi_map.is_locked(OUTER_COMPACT_INSTANCE))
        self.multi_map.lock(OUTER_COMPACT_INSTANCE)
        self.assertTrue(self.multi_map.is_locked(OUTER_COMPACT_INSTANCE))

    def test_force_unlock(self):
        self.multi_map.force_unlock(OUTER_COMPACT_INSTANCE)
        self.multi_map.lock(OUTER_COMPACT_INSTANCE)
        self.multi_map.force_unlock(OUTER_COMPACT_INSTANCE)
        self.assertFalse(self.multi_map.is_locked(OUTER_COMPACT_INSTANCE))

    def test_lock(self):
        self.multi_map.lock(OUTER_COMPACT_INSTANCE)
        self.assertTrue(self.multi_map.is_locked(OUTER_COMPACT_INSTANCE))

    def test_key_set(self):
        self._put_from_another_client(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        self.assertEqual([OUTER_COMPACT_INSTANCE], self.multi_map.key_set())

    def test_remove(self):
        self.assertFalse(self.multi_map.remove(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE))
        self.multi_map.put(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertTrue(self.multi_map.remove(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE))

    def test_remove_all(self):
        self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual(
            [OUTER_COMPACT_INSTANCE], self.multi_map.remove_all(INNER_COMPACT_INSTANCE)
        )

    def test_put(self):
        self.assertTrue(self.multi_map.put(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE))
        self.assertEqual([OUTER_COMPACT_INSTANCE], self.multi_map.get(INNER_COMPACT_INSTANCE))

    def test_put_all(self):
        skip_if_server_version_older_than(self, self.client, "4.1")
        skip_if_client_version_older_than(self, "5.2")
        self.multi_map.put_all({INNER_COMPACT_INSTANCE: [OUTER_COMPACT_INSTANCE]})
        self.assertCountEqual(self.multi_map.get(INNER_COMPACT_INSTANCE), [OUTER_COMPACT_INSTANCE])

    def test_value_count(self):
        self.assertEqual(0, self.multi_map.value_count(OUTER_COMPACT_INSTANCE))
        self.multi_map.put(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        self.assertEqual(1, self.multi_map.value_count(OUTER_COMPACT_INSTANCE))

    def test_try_lock(self):
        self.assertTrue(self.multi_map.try_lock(OUTER_COMPACT_INSTANCE))

    def test_unlock(self):
        try:
            self.multi_map.unlock(OUTER_COMPACT_INSTANCE)
        except IllegalMonitorStateError:
            # Lock is not locked, but we don't care
            # about it, as we only want to verify
            # that we can send the serialized form
            # of key to the server.
            pass

    def test_values(self):
        self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual([OUTER_COMPACT_INSTANCE], self.multi_map.values())

    def _assert_entry_event(self, events):
        self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)

        def assertion():
            self.assertEqual(1, len(events))
            event = events[0]
            self.assertEqual(INNER_COMPACT_INSTANCE, event.key)
            self.assertEqual(OUTER_COMPACT_INSTANCE, event.value)
            self.assertIsNone(event.old_value)
            self.assertIsNone(event.merging_value)

        self.assertTrueEventually(assertion)

    def _put_from_another_client(self, key, value):
        other_client = self.create_client(self.client_config)
        other_client_multi_map = other_client.get_multi_map(self.multi_map.name).blocking()
        other_client_multi_map.put(key, value)


class QueueCompactCompatibilityTest(CompactCompatibilityBase):
    def setUp(self) -> None:
        super().setUp()
        self.queue = self.client.get_queue(random_string()).blocking()

    def tearDown(self) -> None:
        self.queue.destroy()
        super().tearDown()

    def test_add(self):
        self.assertTrue(self.queue.add(OUTER_COMPACT_INSTANCE))

    def test_add_all(self):
        self.assertTrue(self.queue.add_all([INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE]))

    def test_add_listener(self):
        events = []

        def listener(event):
            events.append(event)

        self.queue.add_listener(
            include_value=True,
            item_added_func=listener,
        )

        self._add_from_another_client(OUTER_COMPACT_INSTANCE)

        def assertion():
            self.assertEqual(1, len(events))
            event = events[0]
            self.assertEqual(OUTER_COMPACT_INSTANCE, event.item)

        self.assertTrueEventually(assertion)

    def test_contains(self):
        self.assertFalse(self.queue.contains(OUTER_COMPACT_INSTANCE))
        self.queue.add(OUTER_COMPACT_INSTANCE)
        self.assertTrue(self.queue.contains(OUTER_COMPACT_INSTANCE))

    def test_contains_all(self):
        items = [INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE]
        self.assertFalse(self.queue.contains_all(items))
        self.queue.add_all(items)
        self.assertTrue(self.queue.contains_all(items))

    def test_drain_to(self):
        target = []
        self._add_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual(2, self.queue.drain_to(target))
        self.assertEqual([INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE], target)

    def test_iterator(self):
        self._add_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual([INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE], self.queue.iterator())

    def test_offer(self):
        self.assertTrue(self.queue.offer(OUTER_COMPACT_INSTANCE))

    def test_peek(self):
        self._add_from_another_client(OUTER_COMPACT_INSTANCE)
        self.assertEqual(OUTER_COMPACT_INSTANCE, self.queue.peek())

    def test_poll(self):
        self._add_from_another_client(OUTER_COMPACT_INSTANCE)
        self.assertEqual(OUTER_COMPACT_INSTANCE, self.queue.poll())

    def test_put(self):
        self.queue.put(OUTER_COMPACT_INSTANCE)
        self.assertEqual(OUTER_COMPACT_INSTANCE, self.queue.poll())

    def test_remove(self):
        self.assertFalse(self.queue.remove(OUTER_COMPACT_INSTANCE))
        self.queue.add(OUTER_COMPACT_INSTANCE)
        self.assertTrue(self.queue.remove(OUTER_COMPACT_INSTANCE))

    def test_remove_all(self):
        items = [INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE]
        self.assertFalse(self.queue.remove_all(items))
        self.queue.add_all(items)
        self.assertTrue(self.queue.remove_all(items))

    def test_retain_all(self):
        items = [INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE]
        self.assertFalse(self.queue.retain_all(items))
        self.queue.add_all(items + ["x"])
        self.assertTrue(self.queue.retain_all(items))

    def test_take(self):
        self._add_from_another_client(OUTER_COMPACT_INSTANCE)
        self.assertEqual(OUTER_COMPACT_INSTANCE, self.queue.take())

    def _add_from_another_client(self, *items):
        other_client = self.create_client(self.client_config)
        other_client_queue = other_client.get_queue(self.queue.name).blocking()
        other_client_queue.add_all(items)


class ReliableTopicCompactCompatibilityTest(CompactCompatibilityBase):
    def setUp(self) -> None:
        super().setUp()
        self.reliable_topic = self.client.get_reliable_topic(random_string()).blocking()

    def tearDown(self) -> None:
        self.reliable_topic.destroy()
        super().tearDown()

    def test_add_listener(self):
        messages = []

        def listener(message):
            messages.append(message)

        self.reliable_topic.add_listener(listener)

        self._publish_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)

        def assertion():
            self.assertEqual(2, len(messages))
            self.assertEqual(INNER_COMPACT_INSTANCE, messages[0].message)
            self.assertEqual(OUTER_COMPACT_INSTANCE, messages[1].message)

        self.assertTrueEventually(assertion)

    def test_publish(self):
        messages = []

        def listener(message):
            messages.append(message)

        self.reliable_topic.add_listener(listener)

        self.reliable_topic.publish(OUTER_COMPACT_INSTANCE)

        def assertion():
            self.assertEqual(1, len(messages))
            message = messages[0]
            self.assertEqual(OUTER_COMPACT_INSTANCE, message.message)

        self.assertTrueEventually(assertion)

    def test_publish_all(self):
        messages = []

        def listener(message):
            messages.append(message)

        self.reliable_topic.add_listener(listener)

        self.reliable_topic.publish_all([INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE])

        def assertion():
            self.assertEqual(2, len(messages))
            self.assertEqual(INNER_COMPACT_INSTANCE, messages[0].message)
            self.assertEqual(OUTER_COMPACT_INSTANCE, messages[1].message)

        self.assertTrueEventually(assertion)

    def _publish_from_another_client(self, *messages):
        other_client = self.create_client(self.client_config)
        other_client_reliable_topic = other_client.get_reliable_topic(
            self.reliable_topic.name
        ).blocking()
        other_client_reliable_topic.publish_all(messages)


class ReplicatedMapCompactCompatibilityTest(CompactCompatibilityBase):
    def setUp(self) -> None:
        super().setUp()
        self.replicated_map = self.client.get_replicated_map(random_string()).blocking()

    def tearDown(self) -> None:
        self.replicated_map.destroy()
        super().tearDown()

    def test_add_entry_listener(self):
        events = []

        def listener(event):
            events.append(event)

        self.replicated_map.add_entry_listener(
            added_func=listener,
        )

        self._assert_entry_event(events)

    def test_add_entry_listener_with_key(self):
        events = []

        def listener(event):
            events.append(event)

        self.replicated_map.add_entry_listener(
            key=INNER_COMPACT_INSTANCE,
            added_func=listener,
        )

        self._assert_entry_event(events)

    def test_add_entry_listener_with_key_and_predicate(self):
        events = []

        def listener(event):
            events.append(event)

        self.replicated_map.add_entry_listener(
            key=INNER_COMPACT_INSTANCE,
            predicate=CompactPredicate(),
            added_func=listener,
        )

        self._assert_entry_event(events)

    def test_contains_key(self):
        self.assertFalse(self.replicated_map.contains_key(OUTER_COMPACT_INSTANCE))
        self.replicated_map.put(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        self.assertTrue(self.replicated_map.contains_key(OUTER_COMPACT_INSTANCE))

    def test_contains_value(self):
        self.assertFalse(self.replicated_map.contains_value(OUTER_COMPACT_INSTANCE))
        self.replicated_map.put(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertTrue(self.replicated_map.contains_value(OUTER_COMPACT_INSTANCE))

    def test_entry_set(self):
        self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual(
            [(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)], self.replicated_map.entry_set()
        )

    def test_get(self):
        self.assertIsNone(self.replicated_map.get(OUTER_COMPACT_INSTANCE))
        self.replicated_map.put(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        self.assertEqual(INNER_COMPACT_INSTANCE, self.replicated_map.get(OUTER_COMPACT_INSTANCE))

    def test_key_set(self):
        self._put_from_another_client(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        self.assertEqual([OUTER_COMPACT_INSTANCE], self.replicated_map.key_set())

    def test_put(self):
        self.assertIsNone(self.replicated_map.put(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE))
        self.assertEqual(OUTER_COMPACT_INSTANCE, self.replicated_map.get(INNER_COMPACT_INSTANCE))
        self.assertEqual(
            OUTER_COMPACT_INSTANCE,
            self.replicated_map.put(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE),
        )

    def test_put_all(self):
        self.replicated_map.put_all(
            {
                INNER_COMPACT_INSTANCE: OUTER_COMPACT_INSTANCE,
                OUTER_COMPACT_INSTANCE: INNER_COMPACT_INSTANCE,
            }
        )
        self.assertEqual(OUTER_COMPACT_INSTANCE, self.replicated_map.get(INNER_COMPACT_INSTANCE))
        self.assertEqual(INNER_COMPACT_INSTANCE, self.replicated_map.get(OUTER_COMPACT_INSTANCE))

    def test_remove(self):
        self.assertIsNone(self.replicated_map.remove(OUTER_COMPACT_INSTANCE))
        self.replicated_map.put(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        self.assertEqual(INNER_COMPACT_INSTANCE, self.replicated_map.remove(OUTER_COMPACT_INSTANCE))

    def test_values(self):
        self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual([OUTER_COMPACT_INSTANCE], self.replicated_map.values())

    def _assert_entry_event(self, events):
        self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)

        def assertion():
            self.assertEqual(1, len(events))
            event = events[0]
            self.assertEqual(INNER_COMPACT_INSTANCE, event.key)
            self.assertEqual(OUTER_COMPACT_INSTANCE, event.value)
            self.assertIsNone(event.old_value)
            self.assertIsNone(event.merging_value)

        self.assertTrueEventually(assertion)

    def _put_from_another_client(self, key, value):
        other_client = self.create_client(self.client_config)
        other_client_replicated_map = other_client.get_replicated_map(
            self.replicated_map.name
        ).blocking()
        other_client_replicated_map.put(key, value)


class RingbufferCompactCompatibilityTest(CompactCompatibilityBase):
    def setUp(self) -> None:
        super().setUp()
        self.ringbuffer = self.client.get_ringbuffer(random_string()).blocking()

    def tearDown(self) -> None:
        self.ringbuffer.destroy()
        super().tearDown()

    def test_add(self):
        self.assertEqual(0, self.ringbuffer.add(OUTER_COMPACT_INSTANCE))
        self.assertEqual(OUTER_COMPACT_INSTANCE, self.ringbuffer.read_one(0))

    def test_add_all(self):
        self.assertEqual(
            1, self.ringbuffer.add_all([INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE])
        )
        self.assertEqual(INNER_COMPACT_INSTANCE, self.ringbuffer.read_one(0))
        self.assertEqual(OUTER_COMPACT_INSTANCE, self.ringbuffer.read_one(1))

    def test_read_one(self):
        self._add_from_another_client(OUTER_COMPACT_INSTANCE)
        self.assertEqual(OUTER_COMPACT_INSTANCE, self.ringbuffer.read_one(0))

    def test_read_many(self):
        self._add_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual(
            [INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE],
            self.ringbuffer.read_many(0, 0, 2),
        )

    def test_read_many_with_filter(self):
        self._add_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual(
            [INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE],
            self.ringbuffer.read_many(0, 0, 2, filter=CompactFilter()),
        )

    def _add_from_another_client(self, *items):
        other_client = self.create_client(self.client_config)
        other_client_ringbuffer = other_client.get_ringbuffer(self.ringbuffer.name).blocking()
        other_client_ringbuffer.add_all(items)


class SetCompactCompatibilityTest(CompactCompatibilityBase):
    def setUp(self) -> None:
        super().setUp()
        self.set = self.client.get_set(random_string()).blocking()

    def tearDown(self) -> None:
        self.set.destroy()
        super().tearDown()

    def test_add(self):
        self.assertTrue(self.set.add(OUTER_COMPACT_INSTANCE))

    def test_add_all(self):
        self.assertTrue(self.set.add_all([INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE]))

    def test_add_listener(self):
        events = []

        def listener(event):
            events.append(event)

        self.set.add_listener(
            include_value=True,
            item_added_func=listener,
        )

        self._add_from_another_client(OUTER_COMPACT_INSTANCE)

        def assertion():
            self.assertEqual(1, len(events))
            event = events[0]
            self.assertEqual(OUTER_COMPACT_INSTANCE, event.item)

        self.assertTrueEventually(assertion)

    def test_contains(self):
        self.assertFalse(self.set.contains(OUTER_COMPACT_INSTANCE))
        self.set.add(OUTER_COMPACT_INSTANCE)
        self.assertTrue(self.set.contains(OUTER_COMPACT_INSTANCE))

    def test_contains_all(self):
        self.assertFalse(self.set.contains_all([INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE]))
        self.set.add_all([INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE])
        self.assertTrue(self.set.contains_all([INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE]))

    def test_get_all(self):
        self._add_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertCountEqual([INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE], self.set.get_all())

    def test_remove(self):
        self.assertFalse(self.set.remove(OUTER_COMPACT_INSTANCE))
        self.set.add(OUTER_COMPACT_INSTANCE)
        self.assertTrue(self.set.remove(OUTER_COMPACT_INSTANCE))

    def test_remove_all(self):
        self.assertFalse(self.set.remove_all([INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE]))
        self.set.add_all([INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE])
        self.assertTrue(self.set.remove_all([INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE]))

    def test_retain_all(self):
        items = [INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE]
        self.assertFalse(self.set.retain_all(items))
        self.set.add_all(items + ["x"])
        self.assertTrue(self.set.retain_all(items))

    def _add_from_another_client(self, *items):
        other_client = self.create_client(self.client_config)
        other_client_set = other_client.get_set(self.set.name).blocking()
        other_client_set.add_all(items)


class TopicCompactCompatibilityTest(CompactCompatibilityBase):
    def setUp(self) -> None:
        super().setUp()
        self.topic = self.client.get_topic(random_string()).blocking()

    def tearDown(self) -> None:
        self.topic.destroy()
        super().tearDown()

    def test_add_listener(self):
        events = []

        def listener(event):
            events.append(event)

        self.topic.add_listener(on_message=listener)

        self._publish_from_another_client(OUTER_COMPACT_INSTANCE)

        def assertion():
            self.assertEqual(1, len(events))
            event = events[0]
            self.assertEqual(OUTER_COMPACT_INSTANCE, event.message)

        self.assertTrueEventually(assertion)

    def test_publish(self):
        self.topic.publish(OUTER_COMPACT_INSTANCE)

    def test_publish_all(self):
        skip_if_client_version_older_than(self, "5.2")
        messages = []

        def listener(message):
            messages.append(message)

        self.topic.add_listener(listener)

        self.topic.publish_all([INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE])

        def assertion():
            self.assertEqual(2, len(messages))
            self.assertEqual(INNER_COMPACT_INSTANCE, messages[0].message)
            self.assertEqual(OUTER_COMPACT_INSTANCE, messages[1].message)

        self.assertTrueEventually(assertion)

    def _publish_from_another_client(self, item):
        other_client = self.create_client(self.client_config)
        other_client_topic = other_client.get_topic(self.topic.name).blocking()
        other_client_topic.publish(item)


class TransactionalListCompactCompatibilityTest(CompactCompatibilityBase):
    def setUp(self) -> None:
        super().setUp()
        self.list_name = random_string()
        self.transaction = self.client.new_transaction()
        self.list = self.client.get_list(self.list_name).blocking()

    def tearDown(self) -> None:
        self.list.destroy()
        super().tearDown()

    def test_add(self):
        with self.transaction:
            transactional_list = self._get_transactional_list()
            self.assertTrue(transactional_list.add(OUTER_COMPACT_INSTANCE))

    def test_remove(self):
        self._add_from_another_client(OUTER_COMPACT_INSTANCE)
        with self.transaction:
            transactional_list = self._get_transactional_list()
            self.assertTrue(transactional_list.remove(OUTER_COMPACT_INSTANCE))

    def _get_transactional_list(self):
        return self.transaction.get_list(self.list_name)

    def _add_from_another_client(self, item):
        other_client = self.create_client(self.client_config)
        other_client_list = other_client.get_list(self.list_name).blocking()
        other_client_list.add(item)


class TransactionalMapCompactCompatibilityTest(CompactCompatibilityBase):
    def setUp(self) -> None:
        super().setUp()
        self.map_name = random_string()
        self.transaction = self.client.new_transaction()
        self.map = self.client.get_map(self.map_name).blocking()

    def tearDown(self) -> None:
        self.map.destroy()
        super().tearDown()

    def test_contains_key(self):
        self._put_from_another_client(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        with self.transaction:
            transactional_map = self._get_transactional_map()
            self.assertTrue(transactional_map.contains_key(OUTER_COMPACT_INSTANCE))

    def test_get(self):
        self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        with self.transaction:
            transactional_map = self._get_transactional_map()
            self.assertEqual(OUTER_COMPACT_INSTANCE, transactional_map.get(INNER_COMPACT_INSTANCE))

    def test_get_for_update(self):
        self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        with self.transaction:
            transactional_map = self._get_transactional_map()
            self.assertEqual(
                OUTER_COMPACT_INSTANCE, transactional_map.get_for_update(INNER_COMPACT_INSTANCE)
            )

    def test_put(self):
        with self.transaction:
            transactional_map = self._get_transactional_map()
            self.assertIsNone(transactional_map.put(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE))

        self.assertEqual(OUTER_COMPACT_INSTANCE, self.map.get(INNER_COMPACT_INSTANCE))

    def test_put_if_absent(self):
        with self.transaction:
            transactional_map = self._get_transactional_map()
            self.assertIsNone(
                transactional_map.put_if_absent(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
            )

        self.assertEqual(OUTER_COMPACT_INSTANCE, self.map.get(INNER_COMPACT_INSTANCE))

    def test_set(self):
        with self.transaction:
            transactional_map = self._get_transactional_map()
            self.assertIsNone(transactional_map.set(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE))

        self.assertEqual(OUTER_COMPACT_INSTANCE, self.map.get(INNER_COMPACT_INSTANCE))

    def test_replace(self):
        self._put_from_another_client(INNER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        with self.transaction:
            transactional_map = self._get_transactional_map()
            self.assertEqual(
                INNER_COMPACT_INSTANCE,
                transactional_map.replace(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE),
            )

        self.assertEqual(OUTER_COMPACT_INSTANCE, self.map.get(INNER_COMPACT_INSTANCE))

    def test_replace_if_same(self):
        self._put_from_another_client(INNER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        with self.transaction:
            transactional_map = self._get_transactional_map()
            self.assertTrue(
                transactional_map.replace_if_same(
                    INNER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE
                )
            )

        self.assertEqual(OUTER_COMPACT_INSTANCE, self.map.get(INNER_COMPACT_INSTANCE))

    def test_remove(self):
        self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        with self.transaction:
            transactional_map = self._get_transactional_map()
            self.assertEqual(
                OUTER_COMPACT_INSTANCE, transactional_map.remove(INNER_COMPACT_INSTANCE)
            )

    def test_remove_if_same(self):
        self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        with self.transaction:
            transactional_map = self._get_transactional_map()
            self.assertTrue(
                transactional_map.remove_if_same(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
            )

    def test_delete(self):
        self._put_from_another_client(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        with self.transaction:
            transactional_map = self._get_transactional_map()
            self.assertIsNone(transactional_map.delete(OUTER_COMPACT_INSTANCE))

        self.assertTrue(self.map.is_empty())

    def test_key_set(self):
        self._put_from_another_client(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        with self.transaction:
            transactional_map = self._get_transactional_map()
            self.assertEqual([OUTER_COMPACT_INSTANCE], transactional_map.key_set())

    def test_key_set_with_predicate(self):
        self._put_from_another_client(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        with self.transaction:
            transactional_map = self._get_transactional_map()
            self.assertEqual(
                [OUTER_COMPACT_INSTANCE], transactional_map.key_set(CompactPredicate())
            )

    def test_values(self):
        self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        with self.transaction:
            transactional_map = self._get_transactional_map()
            self.assertEqual([OUTER_COMPACT_INSTANCE], transactional_map.values())

    def test_values_with_predicate(self):
        self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        with self.transaction:
            transactional_map = self._get_transactional_map()
            self.assertEqual([OUTER_COMPACT_INSTANCE], transactional_map.values(CompactPredicate()))

    def _get_transactional_map(self):
        return self.transaction.get_map(self.map_name)

    def _put_from_another_client(self, key, value):
        other_client = self.create_client(self.client_config)
        other_client_map = other_client.get_map(self.map_name).blocking()
        other_client_map.put(key, value)


class TransactionalMultiMapCompactCompatibilityTest(CompactCompatibilityBase):
    def setUp(self) -> None:
        super().setUp()
        self.multi_map_name = random_string()
        self.transaction = self.client.new_transaction()
        self.multi_map = self.client.get_multi_map(self.multi_map_name).blocking()

    def tearDown(self) -> None:
        self.multi_map.destroy()
        super().tearDown()

    def test_put(self):
        with self.transaction:
            transactional_multi_map = self._get_transactional_multi_map()
            self.assertTrue(
                transactional_multi_map.put(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
            )

    def test_get(self):
        self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        with self.transaction:
            transactional_multi_map = self._get_transactional_multi_map()
            self.assertEqual(
                [OUTER_COMPACT_INSTANCE], transactional_multi_map.get(INNER_COMPACT_INSTANCE)
            )

    def test_remove(self):
        self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        with self.transaction:
            transactional_multi_map = self._get_transactional_multi_map()
            self.assertTrue(
                transactional_multi_map.remove(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
            )

    def test_remove_all(self):
        self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        with self.transaction:
            transactional_multi_map = self._get_transactional_multi_map()
            self.assertEqual(
                [OUTER_COMPACT_INSTANCE], transactional_multi_map.remove_all(INNER_COMPACT_INSTANCE)
            )

    def test_value_count(self):
        self._put_from_another_client(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        with self.transaction:
            transactional_multi_map = self._get_transactional_multi_map()
            self.assertEqual(1, transactional_multi_map.value_count(OUTER_COMPACT_INSTANCE))

    def _get_transactional_multi_map(self):
        return self.transaction.get_multi_map(self.multi_map_name)

    def _put_from_another_client(self, key, value):
        other_client = self.create_client(self.client_config)
        other_client_multi_map = other_client.get_multi_map(self.multi_map_name).blocking()
        other_client_multi_map.put(key, value)


class TransactionalQueueCompactCompatibilityTest(CompactCompatibilityBase):
    def setUp(self) -> None:
        super().setUp()
        self.queue_name = random_string()
        self.transaction = self.client.new_transaction()
        self.queue = self.client.get_queue(self.queue_name).blocking()

    def tearDown(self) -> None:
        self.queue.destroy()
        super().tearDown()

    def test_offer(self):
        with self.transaction:
            transactional_queue = self._get_transactional_queue()
            self.assertTrue(transactional_queue.offer(OUTER_COMPACT_INSTANCE))

    def test_take(self):
        self._add_from_another_client(OUTER_COMPACT_INSTANCE)
        with self.transaction:
            transactional_queue = self._get_transactional_queue()
            self.assertEqual(OUTER_COMPACT_INSTANCE, transactional_queue.take())

    def test_poll(self):
        self._add_from_another_client(OUTER_COMPACT_INSTANCE)
        with self.transaction:
            transactional_queue = self._get_transactional_queue()
            self.assertEqual(OUTER_COMPACT_INSTANCE, transactional_queue.poll())

    def test_peek(self):
        self._add_from_another_client(OUTER_COMPACT_INSTANCE)
        with self.transaction:
            transactional_queue = self._get_transactional_queue()
            self.assertEqual(OUTER_COMPACT_INSTANCE, transactional_queue.peek())

    def _get_transactional_queue(self):
        return self.transaction.get_queue(self.queue_name)

    def _add_from_another_client(self, item):
        other_client = self.create_client(self.client_config)
        other_client_queue = other_client.get_queue(self.queue_name).blocking()
        other_client_queue.add(item)


class TransactionalSetCompactCompatibilityTest(CompactCompatibilityBase):
    def setUp(self) -> None:
        super().setUp()
        self.set_name = random_string()
        self.transaction = self.client.new_transaction()
        self.set = self.client.get_set(self.set_name).blocking()

    def tearDown(self) -> None:
        self.set.destroy()
        super().tearDown()

    def test_add(self):
        with self.transaction:
            transactional_set = self._get_transactional_set()
            self.assertTrue(transactional_set.add(OUTER_COMPACT_INSTANCE))

    def test_remove(self):
        self._add_from_another_client(OUTER_COMPACT_INSTANCE)
        with self.transaction:
            transactional_set = self._get_transactional_set()
            self.assertTrue(transactional_set.remove(OUTER_COMPACT_INSTANCE))

    def _get_transactional_set(self):
        return self.transaction.get_set(self.set_name)

    def _add_from_another_client(self, item):
        other_client = self.create_client(self.client_config)
        other_client_set = other_client.get_set(self.set_name).blocking()
        other_client_set.add(item)


class SqlCompactCompatibilityTest(CompactCompatibilityBase):
    def setUp(self) -> None:
        super().setUp()
        self.map_name = random_string()
        self.map = self.client.get_map(self.map_name).blocking()

        self.client.sql.execute(
            f"""
        CREATE MAPPING "{self.map_name}" (
            __key INT,
            stringField VARCHAR
        )
        TYPE IMaP 
        OPTIONS (
            'keyFormat' = 'int',
            'valueFormat' = 'compact',
            'valueCompactTypeName' = 'com.hazelcast.serialization.compact.InnerCompact'
        )
        """
        ).result()

    def tearDown(self) -> None:
        self.map.destroy()
        super().tearDown()

    def test_sql(self):
        self._put_from_another_client(1, OUTER_COMPACT_INSTANCE)
        result = self.client.sql.execute(
            f'SELECT this FROM "{self.map_name}" WHERE ? IS NOT NULL',
            INNER_COMPACT_INSTANCE,
        ).result()

        rows = [row["this"] for row in result]
        self.assertEqual([OUTER_COMPACT_INSTANCE], rows)

    def _put_from_another_client(self, key, value):
        other_client = self.create_client(self.client_config)
        other_client_map = other_client.get_map(self.map_name).blocking()
        other_client_map.put(key, value)


class PartitionServiceCompactCompatibilityTest(CompactCompatibilityBase):
    def test_partition_service(self):
        self.assertEqual(
            267,
            self.client.partition_service.get_partition_id(OUTER_COMPACT_INSTANCE),
        )
