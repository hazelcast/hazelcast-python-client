import math
import typing
import unittest
import uuid

from parameterized import parameterized

from hazelcast.config import Config
from hazelcast.errors import HazelcastSerializationError
from hazelcast.serialization import SerializationServiceV1
from hazelcast.serialization.api import CompactSerializer, CompactReader, CompactWriter, FieldKind
from hazelcast.serialization.compact import (
    RabinFingerprint,
    SchemaWriter,
    FieldDescriptor,
    Schema,
    FIELD_OPERATIONS,
    _BOOLEANS_PER_BYTE,
    SchemaNotReplicatedError,
)


class RabinFingerprintTest(unittest.TestCase):
    @parameterized.expand(
        [
            (100, -5, -6165936963810616235),
            (-9223372036854775808, 0, 36028797018963968),
            (9223372036854775807, 113, -3588673659009074035),
            (-13, -13, 72057594037927935),
            (42, 42, 0),
            (42, -42, -1212835703325587522),
            (0, 0, 0),
            (-123456789, 0, 7049212178818848951),
            (123456789, 127, -8322440716502314713),
            (127, -128, -7333697815154264656),
        ]
    )
    def test_i8_fingerprint(self, fp, value, expected):
        self.assertEqual(expected, RabinFingerprint._of_i8(fp, value))

    @parameterized.expand(
        [
            (-9223372036854775808, 2147483647, 6066553457199370002),
            (9223372036854775807, -2147483648, 6066553459773452525),
            (9223372036854707, 42, -961937498224213201),
            (-42, -42, 4294967295),
            (42, 42, 0),
            (42, -442, 7797744281030715531),
            (0, 0, 0),
            (-123456789, 0, -565582369564281851),
            (123456786669, 42127, 7157681543413310373),
            (2147483647, -2147483648, -7679311364898232185),
        ]
    )
    def test_i32_fingerprint(self, fp, value, expected):
        self.assertEqual(expected, RabinFingerprint._of_i32(fp, value))

    @parameterized.expand(
        [
            (0, "hazelcast", 8164249978089638648),
            (-31231241235, "üğişçö", 6128923854942458838),
            (41231542121235, "😀 😃 😄", -6875080751809013377),
            (RabinFingerprint._EMPTY, "STUdent", 1896492170246289820),
            (RabinFingerprint._EMPTY, "aü😄", -2084249746924383631),
            (RabinFingerprint._EMPTY, "", -2316162475121075004),
            (-123321, "xyz", 2601391163390439688),
            (132132123132132, "    ç", -7699875372487088773),
            (42, "42", 7764866287864698590),
            (-42, "-42", -3434092993477103253),
        ]
    )
    def test_str_fingerprint(self, fp, value, expected):
        self.assertEqual(expected, RabinFingerprint._of_str(fp, value))

    def test_schema(self):
        writer = SchemaWriter("SomeType")
        writer.write_int32("id", 0)
        writer.write_string("name", "")
        writer.write_int8("age", 0)
        writer.write_array_of_timestamp("times", [])
        schema = writer.build()
        self.assertEqual(3662264393229655598, schema.schema_id)


class SchemaTest(unittest.TestCase):
    def test_constructor(self):
        fields = [
            FieldDescriptor(kind.name, kind)
            for kind in FieldKind
            if FIELD_OPERATIONS[kind] is not None
        ]
        schema = Schema("something", fields)
        self._verify_schema(schema, fields)

    def test_with_no_fields(self):
        schema = Schema("something", [])
        self.assertEqual({}, schema.fields)
        self.assertEqual([], schema.fields_list)
        self.assertEqual(0, schema.fix_sized_fields_length)
        self.assertEqual(0, schema.var_sized_field_count)

    def test_with_multiple_booleans(self):
        boolean_count = 100
        boolean_fields = [FieldDescriptor(chr(i), FieldKind.BOOLEAN) for i in range(boolean_count)]

        fields = boolean_fields + [
            FieldDescriptor("fix_sized", FieldKind.INT32),
            FieldDescriptor("var_sized", FieldKind.STRING),
        ]

        schema = Schema("something", fields)
        self.assertEqual(1, schema.var_sized_field_count)

        expected_length = math.ceil(boolean_count / 8) + 4
        self.assertEqual(expected_length, schema.fix_sized_fields_length)

        self.assertEqual(0, schema.fields["fix_sized"].position)
        self.assertEqual(0, schema.fields["var_sized"].index)

        position_so_far = 4
        bit_position_so_far = 0
        for field in boolean_fields:
            if bit_position_so_far == _BOOLEANS_PER_BYTE:
                position_so_far += 1
                bit_position_so_far = 0

            schema_field = schema.fields[field.name]
            self.assertEqual(position_so_far, schema_field.position)
            self.assertEqual(bit_position_so_far, schema_field.bit_position)

            bit_position_so_far += 1

    def _verify_schema(self, schema: Schema, fields: typing.List[FieldDescriptor]):
        self.assertEqual("something", schema.type_name)
        self.assertEqual({f.name: f for f in fields}, schema.fields)
        self.assertCountEqual(fields, schema.fields_list)

        fields.sort(key=lambda f: f.name)

        var_sized_fields = []
        fix_sized_fields = []
        for field in fields:
            op = FIELD_OPERATIONS[field.kind]
            if op is None:
                continue
            if op.is_var_sized():
                var_sized_fields.append(field)
            else:
                fix_sized_fields.append(field)

        fix_sized_fields.sort(key=lambda f: FIELD_OPERATIONS[f.kind].size_in_bytes(), reverse=True)

        fix_sized_fields_length = sum(
            [FIELD_OPERATIONS[f.kind].size_in_bytes() for f in fix_sized_fields]
        )
        fix_sized_fields_length += 1  # For boolean field

        self.assertEqual(len(var_sized_fields), schema.var_sized_field_count)
        self.assertEqual(fix_sized_fields_length, schema.fix_sized_fields_length)

        for i, field in enumerate(var_sized_fields):
            schema_field = schema.fields[field.name]
            self.assertEqual(i, schema_field.index)

            self.assertEqual(-1, schema_field.position)
            self.assertEqual(-1, schema_field.bit_position)

        position_so_far = 0
        for field in fix_sized_fields:
            schema_field = schema.fields[field.name]
            self.assertEqual(position_so_far, schema_field.position)

            if field.kind == FieldKind.BOOLEAN:
                self.assertEqual(0, schema_field.bit_position)
            else:
                self.assertEqual(-1, schema_field.bit_position)

            self.assertEqual(-1, schema_field.index)

            position_so_far += FIELD_OPERATIONS[field.kind].size_in_bytes()


class SchemaWriterTest(unittest.TestCase):
    def test_schema_writer(self):
        writer = SchemaWriter("something")

        fields = []
        for kind in FieldKind:
            name = str(uuid.uuid4())
            fun = getattr(writer, f"write_{kind.name.lower()}", None)
            if fun is None:
                continue
            fun(name, None)
            fields.append((name, kind))

        schema = writer.build()

        for name, kind in fields:
            self.assertEqual(kind, schema.fields.get(name).kind)


class Child:
    def __init__(self, name: str):
        self.name = name


class Parent:
    def __init__(self, child: Child):
        self.child = child


class ChildSerializer(CompactSerializer[Child]):
    def read(self, reader: CompactReader):
        name = reader.read_string("name")
        return Child(name)

    def write(self, writer: CompactWriter, obj: Parent):
        writer.write_string("name", obj.name)

    def get_type_name(self):
        return "Child"

    def get_class(self):
        return Child


class ParentSerializer(CompactSerializer[Parent]):
    def read(self, reader: CompactReader):
        child = reader.read_compact("child")
        return Parent(child)

    def write(self, writer: CompactWriter, obj: Parent):
        writer.write_compact("child", obj.child)

    def get_type_name(self):
        return "Parent"

    def get_class(self):
        return Parent


class NestedSerializerTest(unittest.TestCase):
    def _serialize(self, serialization_service, obj):
        try:
            return serialization_service.to_data(obj)
        except SchemaNotReplicatedError as e:
            serialization_service.compact_stream_serializer.register_schema_to_type(
                e.schema, e.clazz
            )
            return self._serialize(serialization_service, obj)

    def test_missing_serializer(self):
        config = Config()
        config.compact_serializers = [ParentSerializer()]
        service = SerializationServiceV1(config)

        with self.assertRaisesRegex(
            HazelcastSerializationError, "No serializer is registered for class"
        ):
            obj = Parent(Child("test"))
            self._serialize(service, obj)
