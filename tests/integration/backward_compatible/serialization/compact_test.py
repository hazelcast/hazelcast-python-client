import copy
import datetime
import decimal
import itertools
import random

import typing

from parameterized import parameterized

from hazelcast.errors import HazelcastSerializationError
from hazelcast.predicate import sql
from hazelcast.serialization.api import (
    CompactSerializer,
    CompactReader,
    CompactWriter,
)
from hazelcast.serialization.compact import FIELD_OPERATIONS, FieldKind
from hazelcast.util import AtomicInteger
from tests.base import HazelcastTestCase
from tests.util import is_equal, random_string


FIELD_KINDS = [kind for kind in FieldKind]
FIX_SIZED_FIELD_KINDS = [kind for kind in FIELD_KINDS if not FIELD_OPERATIONS[kind].is_var_sized()]
VAR_SIZED_FIELD_KINDS = [kind for kind in FIELD_KINDS if FIELD_OPERATIONS[kind].is_var_sized()]


FIX_SIZED_TO_NULLABLE = {
    FieldKind.BOOLEAN: FieldKind.NULLABLE_BOOLEAN,
    FieldKind.INT8: FieldKind.NULLABLE_INT8,
    FieldKind.INT16: FieldKind.NULLABLE_INT16,
    FieldKind.INT32: FieldKind.NULLABLE_INT32,
    FieldKind.INT64: FieldKind.NULLABLE_INT64,
    FieldKind.FLOAT32: FieldKind.NULLABLE_FLOAT32,
    FieldKind.FLOAT64: FieldKind.NULLABLE_FLOAT64,
}

FIX_SIZED_ARRAY_TO_NULLABLE_FIX_SIZED_ARRAY = {
    FieldKind.ARRAY_OF_BOOLEAN: FieldKind.ARRAY_OF_NULLABLE_BOOLEAN,
    FieldKind.ARRAY_OF_INT8: FieldKind.ARRAY_OF_NULLABLE_INT8,
    FieldKind.ARRAY_OF_INT16: FieldKind.ARRAY_OF_NULLABLE_INT16,
    FieldKind.ARRAY_OF_INT32: FieldKind.ARRAY_OF_NULLABLE_INT32,
    FieldKind.ARRAY_OF_INT64: FieldKind.ARRAY_OF_NULLABLE_INT64,
    FieldKind.ARRAY_OF_FLOAT32: FieldKind.ARRAY_OF_NULLABLE_FLOAT32,
    FieldKind.ARRAY_OF_FLOAT64: FieldKind.ARRAY_OF_NULLABLE_FLOAT64,
}

ARRAY_FIELD_KINDS_WITH_NULLABLE_ITEMS = [
    kind
    for kind in VAR_SIZED_FIELD_KINDS
    if ("ARRAY" in kind.name) and (kind not in FIX_SIZED_ARRAY_TO_NULLABLE_FIX_SIZED_ARRAY)
]


class CompactTestBase(HazelcastTestCase):
    rc = None
    cluster = None
    member = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.rc = cls.create_rc()

        config = """
        <hazelcast xmlns="http://www.hazelcast.com/schema/config"
           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
           xsi:schemaLocation="http://www.hazelcast.com/schema/config
           http://www.hazelcast.com/schema/config/hazelcast-config-5.0.xsd">
            <serialization>
                    <compact-serialization enabled="true" />
            </serialization>
        </hazelcast>
        """

        cls.cluster = cls.create_cluster(cls.rc, config)
        cls.member = cls.cluster.start_member()

    @classmethod
    def tearDownClass(cls):
        cls.rc.terminateCluster(cls.cluster.id)
        cls.rc.exit()

    def tearDown(self) -> None:
        self.shutdown_all_clients()


class CompactTest(CompactTestBase):
    def test_write_then_read_with_all_fields(self):
        serializer = SomeFieldsSerializer.from_kinds(FIELD_KINDS)
        self._write_then_read(FIELD_KINDS, REFERENCE_OBJECTS, serializer)

    def test_write_then_read_with_no_fields(self):
        serializer = SomeFieldsSerializer.from_kinds([])
        self._write_then_read([], {}, serializer)

    def test_write_then_read_with_just_var_sized_fields(self):
        serializer = SomeFieldsSerializer.from_kinds(VAR_SIZED_FIELD_KINDS)
        self._write_then_read(VAR_SIZED_FIELD_KINDS, REFERENCE_OBJECTS, serializer)

    def test_write_then_read_with_just_fix_sized_fields(self):
        serializer = SomeFieldsSerializer.from_kinds(FIX_SIZED_FIELD_KINDS)
        self._write_then_read(FIX_SIZED_FIELD_KINDS, REFERENCE_OBJECTS, serializer)

    @parameterized.expand(
        [
            ("small", 1),
            ("medium", 20),
            ("large", 42),
        ]
    )
    def test_write_then_read_object(self, _, array_item_count):
        reference_objects = {
            FieldKind.ARRAY_OF_STRING: ["x" * (i * 100) for i in range(1, array_item_count)],
            FieldKind.INT32: 32,
            FieldKind.STRING: "hey",
        }
        reference_objects[FieldKind.ARRAY_OF_STRING].append(None)
        serializer = SomeFieldsSerializer.from_kinds(list(reference_objects.keys()))
        self._write_then_read(list(reference_objects.keys()), reference_objects, serializer)

    @parameterized.expand(
        [
            ("0", 0),
            ("1", 1),
            ("8", 8),
            ("10", 10),
            ("100", 100),
            ("1000", 1000),
        ]
    )
    def test_write_then_read_boolean_array(self, _, item_count):
        reference_objects = {
            FieldKind.ARRAY_OF_BOOLEAN: [
                random.randrange(0, 10) % 2 == 0 for _ in range(item_count)
            ]
        }
        serializer = SomeFieldsSerializer.from_kinds(list(reference_objects.keys()))
        self._write_then_read(list(reference_objects.keys()), reference_objects, serializer)

    @parameterized.expand(
        [
            ("0", 0),
            ("1", 1),
            ("8", 8),
            ("10", 10),
            ("100", 100),
            ("1000", 1000),
        ]
    )
    def test_write_and_read_with_multiple_boolean_fields(self, _, field_count):
        all_fields = {str(i): random.randrange(0, 2) % 2 == 0 for i in range(field_count)}

        class Serializer(CompactSerializer[SomeFields]):
            def __init__(self, field_names: typing.List[str]):
                self._field_names = field_names

            def read(self, reader: CompactReader) -> SomeFields:
                fields = {}
                for field_name in self._field_names:
                    fields[field_name] = reader.read_boolean(field_name)

                return SomeFields(**fields)

            def write(self, writer: CompactWriter, obj: SomeFields) -> None:
                for field_name in self._field_names:
                    writer.write_boolean(field_name, getattr(obj, field_name))

            def get_type_name(self) -> str:
                return SomeFields.__name__

        self._write_then_read0(all_fields, Serializer(list(all_fields.keys())))

    @parameterized.expand([(field_kind.name, field_kind) for field_kind in FIELD_KINDS])
    def test_write_then_read(self, _, field_kind):
        field_name = field_kind.name.lower()
        m = self._put_entry(
            map_name=random_string(),
            value_to_put=REFERENCE_OBJECTS[field_kind],
            field_name=field_name,
        )
        obj = m.get("key")
        self.assertTrue(is_equal(REFERENCE_OBJECTS[field_kind], getattr(obj, field_name)))

    @parameterized.expand([(field_kind.name, field_kind) for field_kind in VAR_SIZED_FIELD_KINDS])
    def test_write_none_then_read(self, _, field_kind):
        field_name = field_kind.name.lower()
        m = self._put_entry(
            map_name=random_string(),
            value_to_put=None,
            field_name=field_name,
        )
        obj = m.get("key")
        self.assertIsNone(getattr(obj, field_name))

    @parameterized.expand(
        [(field_kind.name, field_kind) for field_kind in ARRAY_FIELD_KINDS_WITH_NULLABLE_ITEMS]
    )
    def test_write_array_with_none_items_then_read(self, _, field_kind):
        field_name = field_kind.name.lower()
        value = [None] + REFERENCE_OBJECTS[field_kind] + [None]
        value.insert(2, None)
        m = self._put_entry(
            map_name=random_string(),
            value_to_put=value,
            field_name=field_name,
        )
        obj = m.get("key")
        self.assertTrue(is_equal(value, getattr(obj, field_name)))

    @parameterized.expand([(field_kind.name, field_kind) for field_kind in FIELD_KINDS])
    def test_read_when_field_does_not_exist(self, _, field_kind):
        map_name = random_string()
        field_name = field_kind.name.lower()
        self._put_entry(
            map_name=map_name,
            value_to_put=REFERENCE_OBJECTS[field_kind],
            field_name=field_name,
        )

        client = self.create_client(
            {
                "cluster_name": self.cluster.id,
                "compact_serializers": {
                    SomeFieldsSerializer: SomeFieldsSerializer(
                        [
                            FieldDefinition(
                                name=field_name,
                                name_to_read="not-a-field",
                                reader_method_name=f"read_{field_name}",
                            )
                        ]
                    ),
                    Nested: NestedSerializer(),
                },
            }
        )

        evolved_m = client.get_map(map_name).blocking()
        with self.assertRaisesRegex(HazelcastSerializationError, "No field with the name"):
            evolved_m.get("key")

    @parameterized.expand([(field_kind.name, field_kind) for field_kind in FIELD_KINDS])
    def test_read_with_type_mismatch(self, _, field_kind):
        map_name = random_string()
        mismatched_field_kind = FIELD_KINDS[(field_kind.value + 1) % len(FIELD_KINDS)]
        field_name = field_kind.name.lower()
        self._put_entry(
            map_name=map_name,
            value_to_put=REFERENCE_OBJECTS[mismatched_field_kind],
            field_name=field_name,
            writer_method_name=f"write_{mismatched_field_kind.name.lower()}",
        )

        client = self.create_client(
            {
                "cluster_name": self.cluster.id,
                "compact_serializers": {
                    SomeFieldsSerializer: SomeFieldsSerializer([FieldDefinition(name=field_name)]),
                    Nested: NestedSerializer(),
                },
            }
        )

        m = client.get_map(map_name).blocking()
        with self.assertRaisesRegex(HazelcastSerializationError, "Mismatched field types"):
            m.get("key")

    @parameterized.expand(
        [
            (field_kind.name, field_kind, nullable_field_kind)
            for field_kind, nullable_field_kind in itertools.chain(
                FIX_SIZED_TO_NULLABLE.items(),
                FIX_SIZED_ARRAY_TO_NULLABLE_FIX_SIZED_ARRAY.items(),
            )
        ]
    )
    def test_write_fix_sized_then_read_as_nullable_fix_sized(
        self, _, field_kind, nullable_field_kind
    ):
        map_name = random_string()
        field_name = field_kind.name.lower()
        self._put_entry(
            map_name=map_name,
            value_to_put=REFERENCE_OBJECTS[field_kind],
            field_name=field_name,
        )
        nullable_method_suffix = nullable_field_kind.name.lower()
        client = self.create_client(
            {
                "cluster_name": self.cluster.id,
                "compact_serializers": {
                    SomeFieldsSerializer: SomeFieldsSerializer(
                        [
                            FieldDefinition(
                                name=field_name,
                                reader_method_name=f"read_{nullable_method_suffix}",
                            )
                        ]
                    ),
                },
            }
        )

        m = client.get_map(map_name).blocking()
        obj = m.get("key")
        self.assertTrue(is_equal(REFERENCE_OBJECTS[field_kind], getattr(obj, field_name)))

    @parameterized.expand(
        [
            (field_kind.name, field_kind, nullable_field_kind)
            for field_kind, nullable_field_kind in itertools.chain(
                FIX_SIZED_TO_NULLABLE.items(),
                FIX_SIZED_ARRAY_TO_NULLABLE_FIX_SIZED_ARRAY.items(),
            )
        ]
    )
    def test_write_nullable_fix_sized_then_read_as_fix_sized(
        self, _, field_kind, nullable_field_kind
    ):
        map_name = random_string()
        nullable_method_suffix = nullable_field_kind.name.lower()
        field_name = field_kind.name.lower()
        self._put_entry(
            map_name=map_name,
            value_to_put=REFERENCE_OBJECTS[field_kind],
            field_name=field_name,
            writer_method_name=f"write_{nullable_method_suffix}",
        )
        client = self.create_client(
            {
                "cluster_name": self.cluster.id,
                "compact_serializers": {
                    SomeFields: SomeFieldsSerializer([FieldDefinition(name=field_name)]),
                },
            }
        )

        m = client.get_map(map_name).blocking()
        obj = m.get("key")
        self.assertTrue(is_equal(REFERENCE_OBJECTS[field_kind], getattr(obj, field_name)))

    @parameterized.expand(
        [
            (field_kind.name, field_kind, nullable_field_kind)
            for field_kind, nullable_field_kind in FIX_SIZED_TO_NULLABLE.items()
        ]
    )
    def test_write_nullable_fix_sized_as_none_then_read_as_fix_sized(
        self, _, field_kind, nullable_field_kind
    ):
        map_name = random_string()
        nullable_method_suffix = nullable_field_kind.name.lower()
        field_name = field_kind.name.lower()
        self._put_entry(
            map_name=map_name,
            value_to_put=None,
            field_name=field_name,
            writer_method_name=f"write_{nullable_method_suffix}",
        )
        client = self.create_client(
            {
                "cluster_name": self.cluster.id,
                "compact_serializers": {
                    SomeFields: SomeFieldsSerializer([FieldDefinition(name=field_name)]),
                },
            }
        )

        m = client.get_map(map_name).blocking()
        with self.assertRaisesRegex(HazelcastSerializationError, "A 'None' value cannot be read"):
            m.get("key")

    @parameterized.expand(
        [
            (field_kind.name, field_kind, nullable_field_kind)
            for field_kind, nullable_field_kind in FIX_SIZED_ARRAY_TO_NULLABLE_FIX_SIZED_ARRAY.items()
        ]
    )
    def test_write_nullable_fix_sized_array_with_none_item_then_read_as_fix_sized_array(
        self, _, field_kind, nullable_field_kind
    ):
        map_name = random_string()
        nullable_method_suffix = nullable_field_kind.name.lower()
        field_name = field_kind.name.lower()
        self._put_entry(
            map_name=map_name,
            value_to_put=[None],
            field_name=field_name,
            writer_method_name=f"write_{nullable_method_suffix}",
        )
        client = self.create_client(
            {
                "cluster_name": self.cluster.id,
                "compact_serializers": {
                    SomeFields: SomeFieldsSerializer([FieldDefinition(name=field_name)]),
                },
            }
        )

        m = client.get_map(map_name).blocking()
        with self.assertRaisesRegex(HazelcastSerializationError, "A `None` item cannot be read"):
            m.get("key")

    @parameterized.expand([(field_kind.name, field_kind) for field_kind in FIELD_KINDS])
    def test_write_then_read_with_default_value(self, _, field_kind):
        field_name = field_kind.name.lower()
        m = self._put_entry(
            map_name=random_string(),
            value_to_put=REFERENCE_OBJECTS[field_kind],
            field_name=field_name,
            reader_method_name=f"read_{field_name}_or_default",
            default_value_to_read=object(),
        )
        obj = m.get("key")
        self.assertTrue(is_equal(REFERENCE_OBJECTS[field_kind], getattr(obj, field_name)))

    @parameterized.expand([(field_kind.name, field_kind) for field_kind in FIELD_KINDS])
    def test_write_then_read_with_default_value_when_field_name_does_not_match(self, _, field_kind):
        field_name = field_kind.name.lower()
        default_value = object()
        m = self._put_entry(
            map_name=random_string(),
            value_to_put=REFERENCE_OBJECTS[field_kind],
            field_name=field_name,
            field_name_to_read="not-a-field",
            reader_method_name=f"read_{field_name}_or_default",
            default_value_to_read=default_value,
        )
        obj = m.get("key")
        self.assertTrue(getattr(obj, field_name) is default_value)

    @parameterized.expand([(field_kind.name, field_kind) for field_kind in FIELD_KINDS])
    def test_write_then_read_with_default_value_when_field_type_does_not_match(self, _, field_kind):
        field_name = field_kind.name.lower()
        mismatched_field_kind = FIELD_KINDS[(field_kind.value + 1) % len(FIELD_KINDS)]
        default_value = object()
        m = self._put_entry(
            map_name=random_string(),
            value_to_put=REFERENCE_OBJECTS[mismatched_field_kind],
            field_name=field_name,
            field_name_to_read=field_name,
            writer_method_name=f"write_{mismatched_field_kind.name.lower()}",
            reader_method_name=f"read_{field_name}_or_default",
            default_value_to_read=default_value,
        )
        obj = m.get("key")
        self.assertTrue(getattr(obj, field_name) is default_value)

    def _put_entry(
        self,
        *,
        map_name: str,
        value_to_put: typing.Any,
        field_name: str,
        field_name_to_read=None,
        writer_method_name=None,
        reader_method_name=None,
        default_value_to_read=None,
    ):
        field_definition = FieldDefinition(
            name=field_name,
            name_to_read=field_name_to_read or field_name,
            writer_method_name=writer_method_name or f"write_{field_name}",
            reader_method_name=reader_method_name or f"read_{field_name}",
            default_value_to_read=default_value_to_read,
        )
        client = self.create_client(
            {
                "cluster_name": self.cluster.id,
                "compact_serializers": {
                    SomeFields: SomeFieldsSerializer([field_definition]),
                    Nested: NestedSerializer(),
                },
            }
        )

        m = client.get_map(map_name).blocking()
        m.put("key", SomeFields(**{field_name: value_to_put}))
        return m

    def _write_then_read(
        self,
        kinds: typing.List[FieldKind],
        reference_objects: typing.Dict[FieldKind, typing.Any],
        serializer: CompactSerializer,
    ):
        fields = {kind.name.lower(): reference_objects[kind] for kind in kinds}
        self._write_then_read0(fields, serializer)

    def _write_then_read0(
        self, fields: typing.Dict[str, typing.Any], serializer: CompactSerializer
    ):
        client = self.create_client(
            {
                "cluster_name": self.cluster.id,
                "compact_serializers": {
                    SomeFields: serializer,
                    Nested: NestedSerializer(),
                },
            }
        )

        m = client.get_map(random_string()).blocking()
        m.put("key", SomeFields(**fields))
        obj = m.get("key")
        for name, value in fields.items():
            self.assertTrue(is_equal(value, getattr(obj, name)))


class CompactSchemaEvolutionTest(CompactTestBase):
    def test_adding_a_fix_sized_field(self):
        self._verify_adding_a_field(
            ("int32", 42),
            ("string", "42"),
            new_field_name="int64",
            new_field_value=24,
            new_field_default_value=12,
        )

    def test_removing_a_fix_sized_field(self):
        self._verify_removing_a_field(
            ("int64", 1234),
            ("string", "hey"),
            removed_field_name="int64",
            removed_field_default_value=43321,
        )

    def test_adding_a_var_sized_field(self):
        self._verify_adding_a_field(
            ("int32", 42),
            ("string", "42"),
            new_field_name="array_of_boolean",
            new_field_value=[True, False, True],
            new_field_default_value=[False, False, False, True],
        )

    def test_removing_a_var_sized_field(self):
        self._verify_removing_a_field(
            ("int64", 1234),
            ("string", "hey"),
            removed_field_name="string",
            removed_field_default_value="43321",
        )

    def _create_client(self, serializer: CompactSerializer):
        return self.create_client(
            {
                "cluster_name": self.cluster.id,
                "compact_serializers": {
                    SomeFields: serializer,
                },
            }
        )

    def _verify_adding_a_field(
        self,
        *existing_fields: typing.Tuple[str, typing.Any],
        new_field_name: str,
        new_field_value: typing.Any,
        new_field_default_value: typing.Any,
    ):
        map_name = random_string()
        v1_field_definitions = [FieldDefinition(name=name) for name, _ in existing_fields]
        v1_serializer = SomeFieldsSerializer(v1_field_definitions)
        v1_client = self._create_client(v1_serializer)
        v1_map = v1_client.get_map(map_name).blocking()
        v1_fields = {name: value for name, value in existing_fields}
        v1_map.put("key1", SomeFields(**v1_fields))

        v2_field_definitions = v1_field_definitions + [FieldDefinition(name=new_field_name)]
        v2_serializer = SomeFieldsSerializer(v2_field_definitions)
        v2_client = self._create_client(v2_serializer)
        v2_map = v2_client.get_map(map_name).blocking()
        v2_fields = copy.deepcopy(v1_fields)
        v2_fields[new_field_name] = new_field_value
        v2_map.put("key2", SomeFields(**v2_fields))

        careful_v2_field_definitions = v1_field_definitions + [
            FieldDefinition(
                name=new_field_name,
                reader_method_name=f"read_{new_field_name}_or_default",
                default_value_to_read=new_field_default_value,
            )
        ]
        careful_v2_serializer = SomeFieldsSerializer(careful_v2_field_definitions)
        careful_client_v2 = self._create_client(careful_v2_serializer)
        careful_v2_map = careful_client_v2.get_map(map_name).blocking()

        # Old client can read data written by the new client
        v1_obj = v1_map.get("key2")
        for name in v1_fields:
            self.assertEqual(v2_fields[name], getattr(v1_obj, name))

        # New client cannot read data written by the old client, since
        # there is no such field on the old data.

        with self.assertRaisesRegex(HazelcastSerializationError, "No field with the name"):
            v2_map.get("key1")

        # However, if it has default value, everything should work

        careful_v2_obj = careful_v2_map.get("key1")
        for name in v2_fields:
            self.assertEqual(
                v1_fields.get(name) or new_field_default_value,
                getattr(careful_v2_obj, name),
            )

    def _verify_removing_a_field(
        self,
        *existing_fields: typing.Tuple[str, typing.Any],
        removed_field_name: str,
        removed_field_default_value: typing.Any,
    ):
        map_name = random_string()
        v1_field_definitions = [FieldDefinition(name=name) for name, _ in existing_fields]
        v1_serializer = SomeFieldsSerializer(v1_field_definitions)
        v1_client = self._create_client(v1_serializer)
        v1_map = v1_client.get_map(map_name).blocking()
        v1_fields = {name: value for name, value in existing_fields}
        v1_map.put("key1", SomeFields(**v1_fields))

        v2_field_definitions = [
            FieldDefinition(name=name) for name, _ in existing_fields if name != removed_field_name
        ]
        v2_serializer = SomeFieldsSerializer(v2_field_definitions)
        v2_client = self._create_client(v2_serializer)
        v2_map = v2_client.get_map(map_name).blocking()
        v2_fields = copy.deepcopy(v1_fields)
        del v2_fields[removed_field_name]
        v2_map.put("key2", SomeFields(**v2_fields))

        careful_v1_field_definitions = v2_field_definitions + [
            FieldDefinition(
                name=removed_field_name,
                reader_method_name=f"read_{removed_field_name}_or_default",
                default_value_to_read=removed_field_default_value,
            )
        ]
        careful_v1_serializer = SomeFieldsSerializer(careful_v1_field_definitions)
        careful_client_v1 = self._create_client(careful_v1_serializer)
        careful_v1_map = careful_client_v1.get_map(map_name).blocking()

        # Old client cannot read data written by the new client, since
        # there is no such field on the new data

        with self.assertRaisesRegex(HazelcastSerializationError, "No field with the name"):
            v1_map.get("key2")

        # However, if it has default value, everything should work
        v1_obj = careful_v1_map.get("key2")
        for name in v1_fields:
            self.assertEqual(
                v2_fields.get(name) or removed_field_default_value,
                getattr(v1_obj, name),
            )

        # New client can read data written by the old client
        v2_obj = v2_map.get("key1")
        for name in v2_fields:
            self.assertEqual(v1_fields[name], getattr(v2_obj, name))

        with self.assertRaises(AttributeError):
            getattr(v2_obj, removed_field_name)  # no such field for the new schema


class CompactOnClusterRestartTest(CompactTestBase):
    def test_cluster_restart(self):
        client = self.create_client(
            {
                "cluster_name": self.cluster.id,
                "compact_serializers": {
                    SomeFields: SomeFieldsSerializer([FieldDefinition(name="int32")])
                },
            }
        )

        m = client.get_map(random_string()).blocking()
        m.put(1, SomeFields(int32=42))

        self.rc.terminateMember(self.cluster.id, self.member.uuid)
        CompactOnClusterRestartTest.member = self.cluster.start_member()

        m.put(1, SomeFields(int32=42))
        obj = m.get(1)
        self.assertEqual(42, obj.int32)

        # Perform a query to make sure that the schema is available on the cluster
        self.assertEqual(1, len(m.values(sql("int32 == 42"))))


class CompactWithListenerTest(CompactTestBase):
    def test_map_listener(self):
        client = self.create_client(
            {
                "cluster_name": self.cluster.id,
                "compact_serializers": {
                    SomeFields: SomeFieldsSerializer([FieldDefinition(name="int32")])
                },
            }
        )

        m = client.get_map(random_string()).blocking()

        counter = AtomicInteger()

        def listener(entry_event):
            entry_event.value  # Force it to throw SchemaNotFoundError
            # This won't be necessary once we eagerly deserialize
            # the event
            counter.add(1)

        m.add_entry_listener(include_value=True, added_func=listener)
        m.put(1, SomeFields(int32=42))
        self.assertTrueEventually(lambda: self.assertEqual(1, counter.get()))


class SomeFields:
    def __init__(self, **fields):
        self._fields = fields

    def __getattr__(self, item):
        if item not in self._fields:
            raise AttributeError()

        return self._fields[item]


class Nested:
    def __init__(self, i32_field, string_field):
        self.i32_field = i32_field
        self.string_field = string_field

    def __eq__(self, other):
        return (
            isinstance(other, Nested)
            and self.i32_field == other.i32_field
            and self.string_field == other.string_field
        )


class NestedSerializer(CompactSerializer[Nested]):
    def read(self, reader: CompactReader) -> Nested:
        return Nested(reader.read_int32("i32_field"), reader.read_string("string_field"))

    def write(self, writer: CompactWriter, obj: Nested) -> None:
        writer.write_int32("i32_field", obj.i32_field)
        writer.write_string("string_field", obj.string_field)

    def get_type_name(self) -> str:
        return Nested.__name__


class FieldDefinition:
    def __init__(
        self,
        *,
        name: str,
        name_to_read: str = None,
        writer_method_name: str = None,
        reader_method_name: str = None,
        default_value_to_read: typing.Any = None,
    ):
        self.name = name
        self.name_to_read = name_to_read or name
        self.writer_method_name = writer_method_name or f"write_{name}"
        self.reader_method_name = reader_method_name or f"read_{name}"
        self.default_value_to_read = default_value_to_read


class SomeFieldsSerializer(CompactSerializer[SomeFields]):
    def __init__(self, field_definitions: typing.List[FieldDefinition]):
        self._field_definitions = field_definitions

    def read(self, reader: CompactReader) -> SomeFields:
        fields = {}
        for field_definition in self._field_definitions:
            reader_parameters = [field_definition.name_to_read]
            default_value_to_read = field_definition.default_value_to_read
            if default_value_to_read is not None:
                reader_parameters.append(default_value_to_read)

            value = getattr(reader, field_definition.reader_method_name)(*reader_parameters)
            fields[field_definition.name] = value

        return SomeFields(**fields)

    def write(self, writer: CompactWriter, obj: SomeFields) -> None:
        for field_definition in self._field_definitions:
            getattr(writer, field_definition.writer_method_name)(
                field_definition.name,
                getattr(obj, field_definition.name),
            )

    def get_type_name(self) -> str:
        return SomeFields.__name__

    @staticmethod
    def from_kinds(kinds: typing.List[FieldKind]) -> "SomeFieldsSerializer":
        field_definitions = [FieldDefinition(name=kind.name.lower()) for kind in kinds]
        return SomeFieldsSerializer(field_definitions)


REFERENCE_OBJECTS = {
    FieldKind.BOOLEAN: True,
    FieldKind.ARRAY_OF_BOOLEAN: [True, False, True, True, True, False, True, True, False],
    FieldKind.INT8: 42,
    FieldKind.ARRAY_OF_INT8: [42, -128, -1, 127],
    FieldKind.INT16: -456,
    FieldKind.ARRAY_OF_INT16: [-4231, 12343, 0],
    FieldKind.INT32: 21212121,
    FieldKind.ARRAY_OF_INT32: [-1, 1, 0, 9999999],
    FieldKind.INT64: 123456789,
    FieldKind.ARRAY_OF_INT64: [11, -123456789],
    FieldKind.FLOAT32: 12.5,
    FieldKind.ARRAY_OF_FLOAT32: [-13.13, 12345.67, 0.1, 9876543.2, -99999.99],
    FieldKind.FLOAT64: 12345678.90123,
    FieldKind.ARRAY_OF_FLOAT64: [-12345.67],
    FieldKind.STRING: "üğişçöa",
    FieldKind.ARRAY_OF_STRING: ["17", "😊 😇 🙂", "abc"],
    FieldKind.DECIMAL: decimal.Decimal("123.456"),
    FieldKind.ARRAY_OF_DECIMAL: [decimal.Decimal("0"), decimal.Decimal("-123456.789")],
    FieldKind.TIME: datetime.time(2, 3, 4, 5),
    FieldKind.ARRAY_OF_TIME: [datetime.time(8, 7, 6, 5)],
    FieldKind.DATE: datetime.date(2022, 1, 1),
    FieldKind.ARRAY_OF_DATE: [datetime.date(2021, 11, 11), datetime.date(2020, 3, 3)],
    FieldKind.TIMESTAMP: datetime.datetime(2022, 2, 2, 3, 3, 3, 4),
    FieldKind.ARRAY_OF_TIMESTAMP: [datetime.datetime(1990, 2, 12, 13, 14, 54, 98765)],
    FieldKind.TIMESTAMP_WITH_TIMEZONE: datetime.datetime(
        200, 10, 10, 16, 44, 42, 12345, datetime.timezone(datetime.timedelta(hours=2))
    ),
    FieldKind.ARRAY_OF_TIMESTAMP_WITH_TIMEZONE: [
        datetime.datetime(
            2001, 1, 10, 12, 24, 2, 45, datetime.timezone(datetime.timedelta(hours=-2))
        )
    ],
    FieldKind.COMPACT: Nested(42, "42"),
    FieldKind.ARRAY_OF_COMPACT: [Nested(-42, "-42"), Nested(123, "123")],
    FieldKind.NULLABLE_BOOLEAN: False,
    FieldKind.ARRAY_OF_NULLABLE_BOOLEAN: [False, False, True],
    FieldKind.NULLABLE_INT8: 34,
    FieldKind.ARRAY_OF_NULLABLE_INT8: [-32, 32],
    FieldKind.NULLABLE_INT16: 36,
    FieldKind.ARRAY_OF_NULLABLE_INT16: [37, -37, 0, 12345],
    FieldKind.NULLABLE_INT32: -38,
    FieldKind.ARRAY_OF_NULLABLE_INT32: [-39, 2134567, -8765432, 39],
    FieldKind.NULLABLE_INT64: -4040,
    FieldKind.ARRAY_OF_NULLABLE_INT64: [1, 41, -1, 12312312312, -9312912391],
    FieldKind.NULLABLE_FLOAT32: 42.4,
    FieldKind.ARRAY_OF_NULLABLE_FLOAT32: [-43.4, 434.43],
    FieldKind.NULLABLE_FLOAT64: 44.12,
    FieldKind.ARRAY_OF_NULLABLE_FLOAT64: [45.678, -4567.8, 0.12345],
}
