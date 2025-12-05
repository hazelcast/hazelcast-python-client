import copy
import enum
import typing
import unittest

from hazelcast.errors import NullPointerError
from hazelcast.predicate import Predicate
from tests.integration.asyncio.base import HazelcastTestCase
from tests.util import (
    random_string,
    compare_client_version,
    compare_server_version_with_rc,
    skip_if_client_version_older_than,
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
class CompactCompatibilityBase(unittest.IsolatedAsyncioTestCase, HazelcastTestCase):
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

    async def asyncSetUp(self) -> None:
        self.client = await self.create_client(self.client_config)

    async def asyncTearDown(self) -> None:
        await self.shutdown_all_clients()


class MapCompatibilityTest(CompactCompatibilityBase):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.map = await self.client.get_map(random_string())

    async def asyncTearDown(self) -> None:
        await self.map.destroy()
        await super().asyncTearDown()

    async def test_add_entry_listener_with_key_and_predicate(self):
        events = []

        def listener(event):
            events.append(event)

        await self.map.add_entry_listener(
            include_value=True,
            key=INNER_COMPACT_INSTANCE,
            predicate=CompactPredicate(),
            added_func=listener,
        )

        await self._assert_entry_event(events)

    async def test_add_entry_listener_with_predicate(self):
        events = []

        def listener(event):
            events.append(event)

        await self.map.add_entry_listener(
            include_value=True,
            predicate=CompactPredicate(),
            added_func=listener,
        )

        await self._assert_entry_event(events)

    async def test_add_entry_listener_with_key(self):
        events = []

        def listener(event):
            events.append(event)

        await self.map.add_entry_listener(
            include_value=True,
            key=INNER_COMPACT_INSTANCE,
            added_func=listener,
        )

        await self._assert_entry_event(events)

    async def test_add_interceptor(self):
        await self.map.add_interceptor(CompactReturningMapInterceptor())
        self.assertEqual(OUTER_COMPACT_INSTANCE, await self.map.get("non-existent-key"))

    async def test_aggregate(self):
        self.assertEqual(
            OUTER_COMPACT_INSTANCE,
            await self.map.aggregate(CompactReturningAggregator()),
        )

    async def test_aggregate_with_predicate(self):
        self.assertEqual(
            OUTER_COMPACT_INSTANCE,
            await self.map.aggregate(CompactReturningAggregator(), predicate=CompactPredicate()),
        )

    async def test_contains_key(self):
        self.assertFalse(await self.map.contains_key(OUTER_COMPACT_INSTANCE))
        await self.map.put(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        self.assertTrue(await self.map.contains_key(OUTER_COMPACT_INSTANCE))

    async def test_contains_value(self):
        self.assertFalse(await self.map.contains_value(OUTER_COMPACT_INSTANCE))
        await self.map.put(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertTrue(await self.map.contains_value(OUTER_COMPACT_INSTANCE))

    async def test_delete(self):
        await self.map.delete(OUTER_COMPACT_INSTANCE)
        await self.map.put(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        await self.map.delete(OUTER_COMPACT_INSTANCE)
        self.assertIsNone(await self.map.get(OUTER_COMPACT_INSTANCE))

    async def test_entry_set(self):
        await self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual([(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)], await self.map.entry_set())

    async def test_entry_set_with_predicate(self):
        await self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual(
            [(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)],
            await self.map.entry_set(CompactPredicate()),
        )

    async def test_evict(self):
        self.assertFalse(await self.map.evict(OUTER_COMPACT_INSTANCE))
        await self.map.put(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        self.assertTrue(await self.map.evict(OUTER_COMPACT_INSTANCE))

    async def test_execute_on_entries(self):
        await self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual(
            [(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)],
            await self.map.execute_on_entries(CompactReturningEntryProcessor()),
        )

    async def test_execute_on_entries_predicate(self):
        await self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual(
            [(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)],
            await self.map.execute_on_entries(CompactReturningEntryProcessor(), CompactPredicate()),
        )

    async def test_execute_on_key(self):
        await self.map.put(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        self.assertEqual(
            OUTER_COMPACT_INSTANCE,
            await self.map.execute_on_key(OUTER_COMPACT_INSTANCE, CompactReturningEntryProcessor()),
        )

    async def test_execute_on_keys(self):
        await self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual(
            [(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)],
            await self.map.execute_on_keys([INNER_COMPACT_INSTANCE], CompactReturningEntryProcessor()),
        )

    async def test_get(self):
        await self._put_from_another_client(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        self.assertEqual(INNER_COMPACT_INSTANCE, await self.map.get(OUTER_COMPACT_INSTANCE))

    async def test_get_all(self):
        await self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual(
            {INNER_COMPACT_INSTANCE: OUTER_COMPACT_INSTANCE},
            await self.map.get_all([INNER_COMPACT_INSTANCE]),
        )

    async def test_get_entry_view(self):
        await self._put_from_another_client(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        entry_view = await self.map.get_entry_view(OUTER_COMPACT_INSTANCE)
        self.assertEqual(OUTER_COMPACT_INSTANCE, entry_view.key)
        self.assertEqual(INNER_COMPACT_INSTANCE, entry_view.value)

    async def test_key_set(self):
        await self._put_from_another_client(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        self.assertEqual([OUTER_COMPACT_INSTANCE], await self.map.key_set())

    async def test_key_set_with_predicate(self):
        await self._put_from_another_client(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        self.assertEqual(
            [OUTER_COMPACT_INSTANCE],
            await self.map.key_set(CompactPredicate()),
        )

    async def test_load_all_with_keys(self):
        try:
            await self.map.load_all(keys=[INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE])
        except NullPointerError:
            # Since there is no loader configured
            # the server throws this error. In this test,
            # we only care about sending the serialized
            # for of the keys to the server. So, we don't
            # care about what server does with these keys.
            # It should probably handle this gracefully,
            # but it is OK.
            pass

    async def test_project(self):
        await self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual(
            [OUTER_COMPACT_INSTANCE],
            await self.map.project(CompactReturningProjection()),
        )

    async def test_project_with_predicate(self):
        await self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual(
            [OUTER_COMPACT_INSTANCE],
            await self.map.project(CompactReturningProjection(), CompactPredicate()),
        )

    async def test_put(self):
        await self.map.put(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual(OUTER_COMPACT_INSTANCE, await self.map.get(INNER_COMPACT_INSTANCE))

    async def test_put_all(self):
        await self.map.put_all({OUTER_COMPACT_INSTANCE: INNER_COMPACT_INSTANCE})
        self.assertEqual(INNER_COMPACT_INSTANCE, await self.map.get(OUTER_COMPACT_INSTANCE))

    async def test_put_if_absent(self):
        self.assertIsNone(await self.map.put_if_absent(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE))
        self.assertEqual(
            OUTER_COMPACT_INSTANCE,
            await self.map.put_if_absent(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE),
        )

    async def test_put_transient(self):
        await self.map.put_transient(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual(OUTER_COMPACT_INSTANCE, await self.map.get(INNER_COMPACT_INSTANCE))

    async def test_remove(self):
        self.assertIsNone(await self.map.remove(OUTER_COMPACT_INSTANCE))
        await self._put_from_another_client(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        self.assertEqual(INNER_COMPACT_INSTANCE, await self.map.remove(OUTER_COMPACT_INSTANCE))

    async def test_remove_all(self):
        skip_if_client_version_older_than(self, "5.2")

        await self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertIsNone(await self.map.remove_all(CompactPredicate()))
        self.assertEqual(0, await self.map.size())

    async def test_remove_if_same(self):
        self.assertFalse(await self.map.remove_if_same(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE))
        await self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertTrue(await self.map.remove_if_same(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE))

    async def test_replace(self):
        self.assertIsNone(await self.map.replace(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE))
        await self.map.put(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual(
            OUTER_COMPACT_INSTANCE, await self.map.replace(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        )

    async def test_replace_if_same(self):
        self.assertFalse(
            await self.map.replace_if_same(
                INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE
            )
        )
        await self.map.put(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertTrue(
            await self.map.replace_if_same(
                INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE
            )
        )

    async def test_set(self):
        await self.map.set(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual(OUTER_COMPACT_INSTANCE, await self.map.get(INNER_COMPACT_INSTANCE))

    async def test_set_ttl(self):
        await self._put_from_another_client(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        await self.map.set_ttl(OUTER_COMPACT_INSTANCE, 999)

    async def test_try_put(self):
        self.assertTrue(await self.map.try_put(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE))

    async def test_try_remove(self):
        self.assertFalse(await self.map.try_remove(OUTER_COMPACT_INSTANCE))
        await self.map.put(OUTER_COMPACT_INSTANCE, INNER_COMPACT_INSTANCE)
        self.assertTrue(await self.map.try_remove(OUTER_COMPACT_INSTANCE))

    async def test_values(self):
        await self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual([OUTER_COMPACT_INSTANCE], await self.map.values())

    async def test_values_with_predicate(self):
        await self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual([OUTER_COMPACT_INSTANCE], await self.map.values(CompactPredicate()))

    async def _put_from_another_client(self, key, value):
        other_client = await self.create_client(self.client_config)
        other_client_map = await other_client.get_map(self.map.name)
        await other_client_map.put(key, value)

    async def _assert_entry_event(self, events):
        await self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)

        def assertion():
            self.assertEqual(1, len(events))
            event = events[0]
            self.assertEqual(INNER_COMPACT_INSTANCE, event.key)
            self.assertEqual(OUTER_COMPACT_INSTANCE, event.value)
            self.assertIsNone(event.old_value)
            self.assertIsNone(event.merging_value)

        await self.assertTrueEventually(assertion)


class NearCachedMapCompactCompatibilityTest(MapCompatibilityTest):
    async def asyncSetUp(self) -> None:
        map_name = random_string()
        self.client_config = copy.deepcopy(NearCachedMapCompactCompatibilityTest.client_config)
        self.client_config["near_caches"] = {map_name: {}}
        await super().asyncSetUp()
        self.map = await self.client.get_map(map_name)

    async def test_get_for_near_cache(self):
        # Another variant of the test in the superclass, where we lookup a key
        # which has a value whose schema is not fully sent to the
        # cluster from this client. The near cache will try to serialize it,
        # but it should not attempt to send this schema to the cluster, as it
        # is just fetched from there.
        await self._put_from_another_client(INNER_COMPACT_INSTANCE, OUTER_COMPACT_INSTANCE)
        self.assertEqual(OUTER_COMPACT_INSTANCE, await self.map.get(INNER_COMPACT_INSTANCE))


class PartitionServiceCompactCompatibilityTest(CompactCompatibilityBase):
    async def test_partition_service(self):
        self.assertEqual(
            267,
            await self.client.partition_service.get_partition_id(OUTER_COMPACT_INSTANCE),
        )
