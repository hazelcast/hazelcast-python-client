import abc
import collections
import datetime
import decimal
import enum
import typing

from hazelcast.errors import HazelcastError, HazelcastSerializationError, IllegalStateError
from hazelcast.serialization import (
    BYTE_SIZE_IN_BYTES,
    SHORT_SIZE_IN_BYTES,
    INT_SIZE_IN_BYTES,
    LONG_SIZE_IN_BYTES,
    FLOAT_SIZE_IN_BYTES,
    DOUBLE_SIZE_IN_BYTES,
    MAX_BYTE,
    MIN_BYTE,
    MAX_SHORT,
    MIN_SHORT,
)
from hazelcast.serialization.api import (
    CompactSerializer,
    ObjectDataOutput,
    CompactWriter,
    CompactReader,
    ObjectDataInput,
)
from hazelcast.serialization.input import _ObjectDataInput
from hazelcast.serialization.output import _ObjectDataOutput
from hazelcast.serialization.serialization_const import TYPE_COMPACT
from hazelcast.serialization.serializer import BaseSerializer
from hazelcast.serialization.util import IOUtil

_BOOLEANS_PER_BYTE = 8


class CompactStreamSerializer(BaseSerializer):
    def __init__(self, compact_serializers: typing.List[CompactSerializer]):
        self._type_to_serializer = {s.get_class(): s for s in compact_serializers}
        self._type_name_to_serializer = {s.get_type_name(): s for s in compact_serializers}
        self._type_to_schema: typing.Dict[typing.Type, Schema] = {}
        self._id_to_schema: typing.Dict[int, Schema] = {}

    def write(self, out: ObjectDataOutput, obj: typing.Any) -> None:
        clazz = type(obj)
        serializer = self._type_to_serializer[clazz]
        schema = self._type_to_schema.get(clazz)
        if not schema:
            schema = CompactStreamSerializer._build_schema(serializer, obj)
            # Check if we already fetched the schema from the cluster.
            if schema.schema_id not in self._id_to_schema:
                raise SchemaNotReplicatedError(schema, clazz)

            # No need to raise the not replicated error, as we just
            # fetched it from the cluster. Let's register it to our
            # local and continue
            self.register_schema_to_type(schema, clazz)

        out.write_long(schema.schema_id)
        writer = DefaultCompactWriter(self, out, schema)  # type: ignore
        serializer.write(writer, obj)
        writer.write_var_sized_field_positions()

    def read(self, inp: ObjectDataInput) -> typing.Any:
        schema_id = inp.read_long()
        schema = self._id_to_schema.get(schema_id)
        if not schema:
            raise SchemaNotFoundError(schema_id)

        serializer = self._type_name_to_serializer.get(schema.type_name)
        if not serializer:
            raise HazelcastSerializationError(
                f"No compact serializer is registered for the type name '{schema.type_name}'"
            )

        reader = DefaultCompactReader(self, inp, schema)  # type: ignore
        return serializer.read(reader)

    def get_type_id(self) -> int:
        return TYPE_COMPACT

    def register_schema_to_id(self, schema: "Schema") -> None:
        # This method is only called in the reactor thread,
        # as a callback/continuation.
        schema_id = schema.schema_id
        existing_schema = self._id_to_schema.get(schema_id)
        if not existing_schema:
            self._id_to_schema[schema_id] = schema
            return

        if existing_schema != schema:
            raise IllegalStateError(
                f"Schema with the id '{schema_id}' already exists."
                f"Existing schema: {existing_schema}, "
                f"new schema: {schema}."
            )

    def register_schema_to_type(self, schema: "Schema", clazz: typing.Type) -> None:
        # This method is only called in the reactor thread,
        # as a callback/continuation.
        # Put it to local registry to avoid need to fetch it once
        # we read a data with the same schema id
        self.register_schema_to_id(schema)

        existing_schema = self._type_to_schema.get(clazz)
        if not existing_schema:
            self._type_to_schema[clazz] = schema
            return

        if existing_schema != schema:
            raise IllegalStateError(
                f"Schema with the class type '{clazz}' already exists."
                f"Existing schema: {existing_schema}, "
                f"new schema: {schema}."
            )

    def get_schemas(self) -> typing.List["Schema"]:
        return list(self._id_to_schema.values())

    @staticmethod
    def _build_schema(serializer: CompactSerializer, obj: typing.Any) -> "Schema":
        writer = SchemaWriter(serializer.get_type_name())
        serializer.write(writer, obj)
        return writer.build()


T = typing.TypeVar("T")


class DefaultCompactWriter(CompactWriter):
    __slots__ = (
        "_compact_serializer",
        "_out",
        "_schema",
        "_var_sized_field_positions",
        "_data_start_position",
    )

    def __init__(
        self,
        compact_serializer: CompactStreamSerializer,
        out: _ObjectDataOutput,
        schema: "Schema",
    ):
        self._compact_serializer = compact_serializer
        self._out = out
        self._schema = schema

        if schema.var_sized_field_count != 0:
            self._var_sized_field_positions: typing.List[int] = [0] * schema.var_sized_field_count
            self._data_start_position = out.position() + INT_SIZE_IN_BYTES

            # Skip data length (4 bytes) + fix sized fields
            out.write_zero_bytes(schema.fix_sized_fields_length + INT_SIZE_IN_BYTES)
        else:
            self._var_sized_field_positions = []
            self._data_start_position = out.position()

            # Skip fix sized_fields. No need to write data length when
            # there are no var sized fields.
            out.write_zero_bytes(schema.fix_sized_fields_length)

    def write_var_sized_field_positions(self):
        if self._schema.var_sized_field_count == 0:
            return

        position = self._out.position()
        data_length = position - self._data_start_position
        self._write_positions(data_length, self._var_sized_field_positions)
        self._out.write_int_positional(data_length, self._data_start_position - INT_SIZE_IN_BYTES)

    def write_boolean(self, field_name: str, value: bool) -> None:
        field = self._get_field(field_name, FieldKind.BOOLEAN)
        position = field.position + self._data_start_position
        bit_position = field.bit_position
        self._out.write_boolean_bit_positional(value, position, bit_position)

    def write_nullable_boolean(self, field_name: str, value: typing.Optional[bool]) -> None:
        self._write_var_sized_field(
            field_name, FieldKind.NULLABLE_BOOLEAN, value, self._out.write_boolean
        )

    def write_int8(self, field_name: str, value: int) -> None:
        self._write_fix_sized_field(
            field_name, FieldKind.INT8, value, self._out.write_signed_byte_positional
        )

    def write_nullable_int8(self, field_name: str, value: typing.Optional[int]) -> None:
        self._write_var_sized_field(
            field_name, FieldKind.NULLABLE_INT8, value, self._out.write_signed_byte
        )

    def write_int16(self, field_name: str, value: int) -> None:
        self._write_fix_sized_field(
            field_name, FieldKind.INT16, value, self._out.write_short_positional
        )

    def write_nullable_int16(self, field_name: str, value: typing.Optional[int]) -> None:
        self._write_var_sized_field(
            field_name, FieldKind.NULLABLE_INT16, value, self._out.write_short
        )

    def write_int32(self, field_name: str, value: int) -> None:
        self._write_fix_sized_field(
            field_name, FieldKind.INT32, value, self._out.write_int_positional
        )

    def write_nullable_int32(self, field_name: str, value: typing.Optional[int]) -> None:
        self._write_var_sized_field(
            field_name, FieldKind.NULLABLE_INT32, value, self._out.write_int
        )

    def write_int64(self, field_name: str, value: int) -> None:
        self._write_fix_sized_field(
            field_name, FieldKind.INT64, value, self._out.write_long_positional
        )

    def write_nullable_int64(self, field_name: str, value: typing.Optional[int]) -> None:
        self._write_var_sized_field(
            field_name, FieldKind.NULLABLE_INT64, value, self._out.write_long
        )

    def write_float32(self, field_name: str, value: float) -> None:
        self._write_fix_sized_field(
            field_name, FieldKind.FLOAT32, value, self._out.write_float_positional
        )

    def write_nullable_float32(self, field_name: str, value: typing.Optional[float]) -> None:
        self._write_var_sized_field(
            field_name, FieldKind.NULLABLE_FLOAT32, value, self._out.write_float
        )

    def write_float64(self, field_name: str, value: float) -> None:
        self._write_fix_sized_field(
            field_name, FieldKind.FLOAT64, value, self._out.write_double_positional
        )

    def write_nullable_float64(self, field_name: str, value: typing.Optional[float]) -> None:
        self._write_var_sized_field(
            field_name, FieldKind.NULLABLE_FLOAT64, value, self._out.write_double
        )

    def write_string(self, field_name: str, value: typing.Optional[str]) -> None:
        self._write_var_sized_field(field_name, FieldKind.STRING, value, self._out.write_string)

    def write_decimal(self, field_name: str, value: typing.Optional[decimal.Decimal]) -> None:
        self._write_var_sized_field(
            field_name, FieldKind.DECIMAL, value, self._write_decimal_helper
        )

    def write_time(self, field_name: str, value: typing.Optional[datetime.time]) -> None:
        self._write_var_sized_field(field_name, FieldKind.TIME, value, self._write_time_helper)

    def write_date(self, field_name: str, value: typing.Optional[datetime.date]) -> None:
        self._write_var_sized_field(field_name, FieldKind.DATE, value, self._write_date_helper)

    def write_timestamp(self, field_name: str, value: typing.Optional[datetime.datetime]) -> None:
        self._write_var_sized_field(
            field_name, FieldKind.TIMESTAMP, value, self._write_timestamp_helper
        )

    def write_timestamp_with_timezone(
        self, field_name: str, value: typing.Optional[datetime.datetime]
    ) -> None:
        self._write_var_sized_field(
            field_name,
            FieldKind.TIMESTAMP_WITH_TIMEZONE,
            value,
            self._write_timestamp_with_timezone_helper,
        )

    def write_compact(self, field_name: str, value: typing.Optional[typing.Any]) -> None:
        self._write_var_sized_field(
            field_name, FieldKind.COMPACT, value, self._write_compact_helper
        )

    def write_array_of_boolean(
        self, field_name: str, value: typing.Optional[typing.List[bool]]
    ) -> None:
        self._write_var_sized_field(
            field_name, FieldKind.ARRAY_OF_BOOLEAN, value, self._write_boolean_bits
        )

    def write_array_of_nullable_boolean(
        self, field_name: str, value: typing.Optional[typing.List[typing.Optional[bool]]]
    ) -> None:
        self._write_array_of_var_sized_fields(
            field_name, FieldKind.ARRAY_OF_NULLABLE_BOOLEAN, value, self._out.write_boolean
        )

    def write_array_of_int8(
        self, field_name: str, value: typing.Optional[typing.List[int]]
    ) -> None:
        self._write_var_sized_field(
            field_name, FieldKind.ARRAY_OF_INT8, value, self._out.write_signed_byte_array
        )

    def write_array_of_nullable_int8(
        self, field_name: str, value: typing.Optional[typing.List[typing.Optional[int]]]
    ) -> None:
        self._write_array_of_var_sized_fields(
            field_name, FieldKind.ARRAY_OF_NULLABLE_INT8, value, self._out.write_signed_byte
        )

    def write_array_of_int16(
        self, field_name: str, value: typing.Optional[typing.List[int]]
    ) -> None:
        self._write_var_sized_field(
            field_name, FieldKind.ARRAY_OF_INT16, value, self._out.write_short_array
        )

    def write_array_of_nullable_int16(
        self, field_name: str, value: typing.Optional[typing.List[typing.Optional[int]]]
    ) -> None:
        self._write_array_of_var_sized_fields(
            field_name, FieldKind.ARRAY_OF_NULLABLE_INT16, value, self._out.write_short
        )

    def write_array_of_int32(
        self, field_name: str, value: typing.Optional[typing.List[int]]
    ) -> None:
        self._write_var_sized_field(
            field_name, FieldKind.ARRAY_OF_INT32, value, self._out.write_int_array
        )

    def write_array_of_nullable_int32(
        self, field_name: str, value: typing.Optional[typing.List[typing.Optional[int]]]
    ) -> None:
        self._write_array_of_var_sized_fields(
            field_name, FieldKind.ARRAY_OF_NULLABLE_INT32, value, self._out.write_int
        )

    def write_array_of_int64(
        self, field_name: str, value: typing.Optional[typing.List[int]]
    ) -> None:
        self._write_var_sized_field(
            field_name, FieldKind.ARRAY_OF_INT64, value, self._out.write_long_array
        )

    def write_array_of_nullable_int64(
        self, field_name: str, value: typing.Optional[typing.List[typing.Optional[int]]]
    ) -> None:
        self._write_array_of_var_sized_fields(
            field_name, FieldKind.ARRAY_OF_NULLABLE_INT64, value, self._out.write_long
        )

    def write_array_of_float32(
        self, field_name: str, value: typing.Optional[typing.List[float]]
    ) -> None:
        self._write_var_sized_field(
            field_name, FieldKind.ARRAY_OF_FLOAT32, value, self._out.write_float_array
        )

    def write_array_of_nullable_float32(
        self, field_name: str, value: typing.Optional[typing.List[typing.Optional[float]]]
    ) -> None:
        self._write_array_of_var_sized_fields(
            field_name, FieldKind.ARRAY_OF_NULLABLE_FLOAT32, value, self._out.write_float
        )

    def write_array_of_float64(
        self, field_name: str, value: typing.Optional[typing.List[float]]
    ) -> None:
        self._write_var_sized_field(
            field_name, FieldKind.ARRAY_OF_FLOAT64, value, self._out.write_double_array
        )

    def write_array_of_nullable_float64(
        self, field_name: str, value: typing.Optional[typing.List[typing.Optional[float]]]
    ) -> None:
        self._write_array_of_var_sized_fields(
            field_name, FieldKind.ARRAY_OF_NULLABLE_FLOAT64, value, self._out.write_double
        )

    def write_array_of_string(
        self, field_name: str, value: typing.Optional[typing.List[typing.Optional[str]]]
    ) -> None:
        self._write_array_of_var_sized_fields(
            field_name, FieldKind.ARRAY_OF_STRING, value, self._out.write_string
        )

    def write_array_of_decimal(
        self, field_name: str, value: typing.Optional[typing.List[typing.Optional[decimal.Decimal]]]
    ) -> None:
        self._write_array_of_var_sized_fields(
            field_name, FieldKind.ARRAY_OF_DECIMAL, value, self._write_decimal_helper
        )

    def write_array_of_time(
        self, field_name: str, value: typing.Optional[typing.List[typing.Optional[datetime.time]]]
    ) -> None:
        self._write_array_of_var_sized_fields(
            field_name, FieldKind.ARRAY_OF_TIME, value, self._write_time_helper
        )

    def write_array_of_date(
        self, field_name: str, value: typing.Optional[typing.List[typing.Optional[datetime.date]]]
    ) -> None:
        self._write_array_of_var_sized_fields(
            field_name, FieldKind.ARRAY_OF_DATE, value, self._write_date_helper
        )

    def write_array_of_timestamp(
        self,
        field_name: str,
        value: typing.Optional[typing.List[typing.Optional[datetime.datetime]]],
    ) -> None:
        self._write_array_of_var_sized_fields(
            field_name, FieldKind.ARRAY_OF_TIMESTAMP, value, self._write_timestamp_helper
        )

    def write_array_of_timestamp_with_timezone(
        self,
        field_name: str,
        value: typing.Optional[typing.List[typing.Optional[datetime.datetime]]],
    ) -> None:
        self._write_array_of_var_sized_fields(
            field_name,
            FieldKind.ARRAY_OF_TIMESTAMP_WITH_TIMEZONE,
            value,
            self._write_timestamp_with_timezone_helper,
        )

    def write_array_of_compact(
        self, field_name: str, value: typing.Optional[typing.List[typing.Optional[typing.Any]]]
    ) -> None:
        self._write_array_of_var_sized_fields(
            field_name, FieldKind.ARRAY_OF_COMPACT, value, self._write_compact_helper
        )

    def _write_compact_helper(self, value: typing.Any) -> None:
        self._compact_serializer.write(self._out, value)

    def _write_timestamp_with_timezone_helper(self, value: datetime.datetime) -> None:
        IOUtil.write_timestamp_with_timezone(self._out, value)

    def _write_timestamp_helper(self, value: datetime.datetime) -> None:
        IOUtil.write_timestamp(self._out, value)

    def _write_date_helper(self, value: datetime.date) -> None:
        IOUtil.write_date(self._out, value)

    def _write_time_helper(self, value: datetime.time) -> None:
        IOUtil.write_time(self._out, value)

    def _write_decimal_helper(self, value: decimal.Decimal) -> None:
        IOUtil.write_big_decimal(self._out, value)

    def _write_positions(self, data_length: int, positions: typing.List[int]):
        if data_length < PositionReader.UINT8_POSITION_READER_RANGE:
            for position in positions:
                self._out.write_byte(position & 0xFF)
        elif data_length < PositionReader.UINT16_POSITION_READER_RANGE:
            for position in positions:
                self._out.write_unsigned_short(position & 0xFFFF)
        else:
            for position in positions:
                self._out.write_int(position)

    def _get_field(self, field_name: str, field_kind: "FieldKind") -> "FieldDescriptor":
        field = self._schema.fields.get(field_name, None)
        if not field:
            raise HazelcastSerializationError(
                f"No field with the name '{field_name}' can found in the {self._schema}"
            )

        if field.kind != field_kind:
            raise HazelcastSerializationError(
                f"Mismatched field types. Expected: {field_kind}, found: {field.kind}"
            )

        return field

    def _write_var_sized_field(
        self,
        field_name: str,
        field_kind: "FieldKind",
        value: typing.Optional[T],
        writer: typing.Callable[[T], None],
    ) -> None:
        if value is None:
            self._set_position_as_none(field_name, field_kind)
            return

        self._set_position(field_name, field_kind)
        writer(value)

    def _write_fix_sized_field(
        self,
        field_name: str,
        field_kind: "FieldKind",
        value: T,
        writer: typing.Callable[[T, int], None],
    ):
        position = self._get_fix_sized_field_position(field_name, field_kind)
        writer(value, position)

    def _set_position(self, field_name: str, field_kind: "FieldKind") -> None:
        field = self._get_field(field_name, field_kind)
        position = self._out.position()
        field_position = position - self._data_start_position
        index = field.index
        self._var_sized_field_positions[index] = field_position

    def _set_position_as_none(self, field_name: str, field_kind: "FieldKind") -> None:
        field = self._get_field(field_name, field_kind)
        index = field.index
        self._var_sized_field_positions[index] = PositionReader.NULL_POSITION

    def _get_fix_sized_field_position(self, field_name: str, field_kind: "FieldKind") -> int:
        field = self._get_field(field_name, field_kind)
        return self._data_start_position + field.position

    def _write_boolean_bits(self, value: typing.List[bool]) -> None:
        out = self._out
        n = len(value)
        out.write_int(n)
        position = out.position()

        bit_position = 0
        if n > 0:
            out.write_zero_bytes(1)

        for b in value:
            if bit_position == _BOOLEANS_PER_BYTE:
                bit_position = 0
                out.write_zero_bytes(1)
                position += 1

            out.write_boolean_bit_positional(b, position, bit_position)
            bit_position += 1

    def _write_array_of_var_sized_fields(
        self,
        field_name: str,
        field_kind: "FieldKind",
        value: typing.Optional[typing.List[typing.Optional[T]]],
        item_writer: typing.Callable[[T], None],
    ):
        if value is None:
            self._set_position_as_none(field_name, field_kind)
            return

        out = self._out

        self._set_position(field_name, field_kind)
        data_length_position = out.position()
        out.write_zero_bytes(INT_SIZE_IN_BYTES)
        item_count = len(value)
        out.write_int(item_count)

        data_position = out.position()
        positions = [PositionReader.NULL_POSITION] * item_count
        for index, item in enumerate(value):
            if item is not None:
                positions[index] = out.position() - data_position
                item_writer(item)

        data_length = out.position() - data_position
        out.write_int_positional(data_length, data_length_position)
        self._write_positions(data_length, positions)


class DefaultCompactReader(CompactReader):
    __slots__ = (
        "_compact_serializer",
        "_inp",
        "_schema",
        "_data_start_position",
        "_var_sized_field_positions_position",
        "_position_reader",
    )

    def __init__(
        self,
        compact_serializer: CompactStreamSerializer,
        inp: _ObjectDataInput,
        schema: "Schema",
    ):
        self._compact_serializer = compact_serializer
        self._inp = inp
        self._schema = schema

        var_sized_field_count = schema.var_sized_field_count
        if var_sized_field_count != 0:
            data_length = inp.read_int()
            self._data_start_position = inp.position()
            self._var_sized_field_positions_position = self._data_start_position + data_length
            if data_length < PositionReader.UINT8_POSITION_READER_RANGE:
                self._position_reader: PositionReader = _UINT8_POSITION_READER_INSTANCE
                end_position = self._var_sized_field_positions_position + var_sized_field_count
            elif data_length < PositionReader.UINT16_POSITION_READER_RANGE:
                self._position_reader = _UINT16_POSITION_READER_INSTANCE
                end_position = (
                    self._var_sized_field_positions_position
                    + var_sized_field_count * SHORT_SIZE_IN_BYTES
                )
            else:
                self._position_reader = _INT32_POSITION_READER_INSTANCE
                end_position = (
                    self._var_sized_field_positions_position
                    + var_sized_field_count * INT_SIZE_IN_BYTES
                )
        else:
            self._position_reader = _INT32_POSITION_READER_INSTANCE
            self._var_sized_field_positions_position = 0
            self._data_start_position = inp.position()
            end_position = self._data_start_position + schema.fix_sized_fields_length

        # set the position to the end so that the next one to read something
        # from inp can start from the correct position
        inp.set_position(end_position)

    def get_field_kind(self, field_name) -> "FieldKind":
        field = self._schema.fields.get(field_name)
        if not field:
            return FieldKind.NOT_AVAILABLE
        return field.kind

    def read_boolean(self, field_name: str) -> bool:
        field = self._get_field(field_name)
        kind = field.kind
        if kind == FieldKind.BOOLEAN:
            return self._read_boolean_internal(field)
        elif kind == FieldKind.NULLABLE_BOOLEAN:
            return self._read_var_sized_field_non_none(field, self._inp.read_boolean)
        else:
            raise DefaultCompactReader._create_mismatched_field_kind_error(
                field_name, kind, FieldKind.BOOLEAN
            )

    def read_boolean_or_default(self, field_name: str, default: bool) -> bool:
        if not self._is_field_exists(field_name, FieldKind.BOOLEAN):
            return default

        return self.read_boolean(field_name)

    def read_nullable_boolean(self, field_name: str) -> typing.Optional[bool]:
        field = self._get_field(field_name)
        kind = field.kind
        if kind == FieldKind.BOOLEAN:
            return self._read_boolean_internal(field)
        elif kind == FieldKind.NULLABLE_BOOLEAN:
            return self._read_var_sized_field(field, self._inp.read_boolean)
        else:
            raise DefaultCompactReader._create_mismatched_field_kind_error(
                field_name, kind, FieldKind.NULLABLE_BOOLEAN
            )

    def read_nullable_boolean_or_default(
        self, field_name: str, default: typing.Optional[bool]
    ) -> typing.Optional[bool]:
        if not self._is_field_exists(field_name, FieldKind.NULLABLE_BOOLEAN):
            return default

        return self.read_nullable_boolean(field_name)

    def read_int8(self, field_name: str) -> int:
        field = self._get_field(field_name)
        kind = field.kind
        if kind == FieldKind.INT8:
            return self._inp.read_byte_positional(field.position + self._data_start_position)
        elif kind == FieldKind.NULLABLE_INT8:
            return self._read_var_sized_field_non_none(field, self._inp.read_byte)
        else:
            raise DefaultCompactReader._create_mismatched_field_kind_error(
                field_name, kind, FieldKind.INT8
            )

    def read_int8_or_default(self, field_name: str, default: int) -> int:
        if not self._is_field_exists(field_name, FieldKind.INT8):
            return default

        return self.read_int8(field_name)

    def read_nullable_int8(self, field_name: str) -> typing.Optional[int]:
        field = self._get_field(field_name)
        kind = field.kind
        if kind == FieldKind.INT8:
            return self._inp.read_byte_positional(field.position + self._data_start_position)
        elif kind == FieldKind.NULLABLE_INT8:
            return self._read_var_sized_field(field, self._inp.read_byte)
        else:
            raise DefaultCompactReader._create_mismatched_field_kind_error(
                field_name, kind, FieldKind.NULLABLE_INT8
            )

    def read_nullable_int8_or_default(
        self, field_name: str, default: typing.Optional[int]
    ) -> typing.Optional[int]:
        if not self._is_field_exists(field_name, FieldKind.NULLABLE_INT8):
            return default

        return self.read_nullable_int8(field_name)

    def read_int16(self, field_name: str) -> int:
        field = self._get_field(field_name)
        kind = field.kind
        if kind == FieldKind.INT16:
            return self._inp.read_short_positional(field.position + self._data_start_position)
        elif kind == FieldKind.NULLABLE_INT16:
            return self._read_var_sized_field_non_none(field, self._inp.read_short)
        else:
            raise DefaultCompactReader._create_mismatched_field_kind_error(
                field_name, kind, FieldKind.INT16
            )

    def read_int16_or_default(self, field_name: str, default: int) -> int:
        if not self._is_field_exists(field_name, FieldKind.INT16):
            return default

        return self.read_int16(field_name)

    def read_nullable_int16(self, field_name: str) -> typing.Optional[int]:
        field = self._get_field(field_name)
        kind = field.kind
        if kind == FieldKind.INT16:
            return self._inp.read_short_positional(field.position + self._data_start_position)
        elif kind == FieldKind.NULLABLE_INT16:
            return self._read_var_sized_field(field, self._inp.read_short)
        else:
            raise DefaultCompactReader._create_mismatched_field_kind_error(
                field_name, kind, FieldKind.NULLABLE_INT16
            )

    def read_nullable_int16_or_default(
        self, field_name: str, default: typing.Optional[int]
    ) -> typing.Optional[int]:
        if not self._is_field_exists(field_name, FieldKind.NULLABLE_INT16):
            return default

        return self.read_nullable_int16(field_name)

    def read_int32(self, field_name: str) -> int:
        field = self._get_field(field_name)
        kind = field.kind
        if kind == FieldKind.INT32:
            return self._inp.read_int_positional(field.position + self._data_start_position)
        elif kind == FieldKind.NULLABLE_INT32:
            return self._read_var_sized_field_non_none(field, self._inp.read_int)
        else:
            raise DefaultCompactReader._create_mismatched_field_kind_error(
                field_name, kind, FieldKind.INT32
            )

    def read_int32_or_default(self, field_name: str, default: int) -> int:
        if not self._is_field_exists(field_name, FieldKind.INT32):
            return default

        return self.read_int32(field_name)

    def read_nullable_int32(self, field_name: str) -> typing.Optional[int]:
        field = self._get_field(field_name)
        kind = field.kind
        if kind == FieldKind.INT32:
            return self._inp.read_int_positional(field.position + self._data_start_position)
        elif kind == FieldKind.NULLABLE_INT32:
            return self._read_var_sized_field(field, self._inp.read_int)
        else:
            raise DefaultCompactReader._create_mismatched_field_kind_error(
                field_name, kind, FieldKind.NULLABLE_INT32
            )

    def read_nullable_int32_or_default(
        self, field_name: str, default: typing.Optional[int]
    ) -> typing.Optional[int]:
        if not self._is_field_exists(field_name, FieldKind.NULLABLE_INT32):
            return default

        return self.read_nullable_int32(field_name)

    def read_int64(self, field_name: str) -> int:
        field = self._get_field(field_name)
        kind = field.kind
        if kind == FieldKind.INT64:
            return self._inp.read_long_positional(field.position + self._data_start_position)
        elif kind == FieldKind.NULLABLE_INT64:
            return self._read_var_sized_field_non_none(field, self._inp.read_long)
        else:
            raise DefaultCompactReader._create_mismatched_field_kind_error(
                field_name, kind, FieldKind.INT64
            )

    def read_int64_or_default(self, field_name: str, default: int) -> int:
        if not self._is_field_exists(field_name, FieldKind.INT64):
            return default

        return self.read_int64(field_name)

    def read_nullable_int64(self, field_name: str) -> typing.Optional[int]:
        field = self._get_field(field_name)
        kind = field.kind
        if kind == FieldKind.INT64:
            return self._inp.read_long_positional(field.position + self._data_start_position)
        elif kind == FieldKind.NULLABLE_INT64:
            return self._read_var_sized_field(field, self._inp.read_long)
        else:
            raise DefaultCompactReader._create_mismatched_field_kind_error(
                field_name, kind, FieldKind.NULLABLE_INT64
            )

    def read_nullable_int64_or_default(
        self, field_name: str, default: typing.Optional[int]
    ) -> typing.Optional[int]:
        if not self._is_field_exists(field_name, FieldKind.NULLABLE_INT64):
            return default

        return self.read_nullable_int64(field_name)

    def read_float32(self, field_name: str) -> float:
        field = self._get_field(field_name)
        kind = field.kind
        if kind == FieldKind.FLOAT32:
            return self._inp.read_float_positional(field.position + self._data_start_position)
        elif kind == FieldKind.NULLABLE_FLOAT32:
            return self._read_var_sized_field_non_none(field, self._inp.read_float)
        else:
            raise DefaultCompactReader._create_mismatched_field_kind_error(
                field_name, kind, FieldKind.FLOAT32
            )

    def read_float32_or_default(self, field_name: str, default: float) -> float:
        if not self._is_field_exists(field_name, FieldKind.FLOAT32):
            return default

        return self.read_float32(field_name)

    def read_nullable_float32(self, field_name: str) -> typing.Optional[float]:
        field = self._get_field(field_name)
        kind = field.kind
        if kind == FieldKind.FLOAT32:
            return self._inp.read_float_positional(field.position + self._data_start_position)
        elif kind == FieldKind.NULLABLE_FLOAT32:
            return self._read_var_sized_field(field, self._inp.read_float)
        else:
            raise DefaultCompactReader._create_mismatched_field_kind_error(
                field_name, kind, FieldKind.NULLABLE_FLOAT32
            )

    def read_nullable_float32_or_default(
        self, field_name: str, default: typing.Optional[float]
    ) -> typing.Optional[float]:
        if not self._is_field_exists(field_name, FieldKind.NULLABLE_FLOAT32):
            return default

        return self.read_nullable_float32(field_name)

    def read_float64(self, field_name: str) -> float:
        field = self._get_field(field_name)
        kind = field.kind
        if kind == FieldKind.FLOAT64:
            return self._inp.read_double_positional(field.position + self._data_start_position)
        elif kind == FieldKind.NULLABLE_FLOAT64:
            return self._read_var_sized_field_non_none(field, self._inp.read_double)
        else:
            raise DefaultCompactReader._create_mismatched_field_kind_error(
                field_name, kind, FieldKind.FLOAT64
            )

    def read_float64_or_default(self, field_name: str, default: float) -> float:
        if not self._is_field_exists(field_name, FieldKind.FLOAT64):
            return default

        return self.read_float64(field_name)

    def read_nullable_float64(self, field_name: str) -> typing.Optional[float]:
        field = self._get_field(field_name)
        kind = field.kind
        if kind == FieldKind.FLOAT64:
            return self._inp.read_double_positional(field.position + self._data_start_position)
        elif kind == FieldKind.NULLABLE_FLOAT64:
            return self._read_var_sized_field(field, self._inp.read_double)
        else:
            raise DefaultCompactReader._create_mismatched_field_kind_error(
                field_name, kind, FieldKind.NULLABLE_FLOAT64
            )

    def read_nullable_float64_or_default(
        self, field_name: str, default: typing.Optional[float]
    ) -> typing.Optional[float]:
        if not self._is_field_exists(field_name, FieldKind.NULLABLE_FLOAT64):
            return default

        return self.read_nullable_float64(field_name)

    def read_string(self, field_name: str) -> typing.Optional[str]:
        field = self._get_field_with_kind_check(field_name, FieldKind.STRING)
        return self._read_var_sized_field(field, self._inp.read_string)

    def read_string_or_default(
        self, field_name: str, default: typing.Optional[str]
    ) -> typing.Optional[str]:
        if not self._is_field_exists(field_name, FieldKind.STRING):
            return default

        return self.read_string(field_name)

    def read_decimal(self, field_name: str) -> typing.Optional[decimal.Decimal]:
        field = self._get_field_with_kind_check(field_name, FieldKind.DECIMAL)
        return self._read_var_sized_field(field, self._read_decimal_helper)

    def read_decimal_or_default(
        self, field_name: str, default: typing.Optional[decimal.Decimal]
    ) -> typing.Optional[decimal.Decimal]:
        if not self._is_field_exists(field_name, FieldKind.DECIMAL):
            return default

        return self.read_decimal(field_name)

    def read_time(self, field_name: str) -> typing.Optional[datetime.time]:
        field = self._get_field_with_kind_check(field_name, FieldKind.TIME)
        return self._read_var_sized_field(field, self._read_time_helper)

    def read_time_or_default(
        self, field_name: str, default: typing.Optional[datetime.time]
    ) -> typing.Optional[datetime.time]:
        if not self._is_field_exists(field_name, FieldKind.TIME):
            return default

        return self.read_time(field_name)

    def read_date(self, field_name: str) -> typing.Optional[datetime.date]:
        field = self._get_field_with_kind_check(field_name, FieldKind.DATE)
        return self._read_var_sized_field(field, self._read_date_helper)

    def read_date_or_default(
        self, field_name: str, default: typing.Optional[datetime.date]
    ) -> typing.Optional[datetime.date]:
        if not self._is_field_exists(field_name, FieldKind.DATE):
            return default

        return self.read_date(field_name)

    def read_timestamp(self, field_name: str) -> typing.Optional[datetime.datetime]:
        field = self._get_field_with_kind_check(field_name, FieldKind.TIMESTAMP)
        return self._read_var_sized_field(field, self._read_timestamp_helper)

    def read_timestamp_or_default(
        self, field_name: str, default: typing.Optional[datetime.datetime]
    ) -> typing.Optional[datetime.datetime]:
        if not self._is_field_exists(field_name, FieldKind.TIMESTAMP):
            return default

        return self.read_timestamp(field_name)

    def read_timestamp_with_timezone(self, field_name: str) -> typing.Optional[datetime.datetime]:
        field = self._get_field_with_kind_check(field_name, FieldKind.TIMESTAMP_WITH_TIMEZONE)
        return self._read_var_sized_field(field, self._read_timestamp_with_timezone_helper)

    def read_timestamp_with_timezone_or_default(
        self, field_name: str, default: typing.Optional[datetime.datetime]
    ) -> typing.Optional[datetime.datetime]:
        if not self._is_field_exists(field_name, FieldKind.TIMESTAMP_WITH_TIMEZONE):
            return default

        return self.read_timestamp_with_timezone(field_name)

    def read_compact(self, field_name: str) -> typing.Optional[typing.Any]:
        field = self._get_field_with_kind_check(field_name, FieldKind.COMPACT)
        return self._read_var_sized_field(field, self._read_compact_helper)

    def read_compact_or_default(
        self, field_name: str, default: typing.Optional[typing.Any]
    ) -> typing.Optional[typing.Any]:
        if not self._is_field_exists(field_name, FieldKind.COMPACT):
            return default

        return self.read_compact(field_name)

    def read_array_of_boolean(self, field_name: str) -> typing.Optional[typing.List[bool]]:
        field = self._get_field(field_name)
        kind = field.kind
        if kind == FieldKind.ARRAY_OF_BOOLEAN:
            return self._read_var_sized_field(field, self._read_boolean_bits)
        elif kind == FieldKind.ARRAY_OF_NULLABLE_BOOLEAN:
            return self._read_nullable_item_array_as_fix_sized_item_array(
                field, self._inp.read_boolean_array
            )
        else:
            raise DefaultCompactReader._create_mismatched_field_kind_error(
                field_name, kind, FieldKind.ARRAY_OF_BOOLEAN
            )

    def read_array_of_boolean_or_default(
        self, field_name: str, default: typing.Optional[typing.List[bool]]
    ) -> typing.Optional[typing.List[bool]]:
        if not self._is_field_exists(field_name, FieldKind.ARRAY_OF_BOOLEAN):
            return default

        return self.read_array_of_boolean(field_name)

    def read_array_of_nullable_boolean(
        self, field_name: str
    ) -> typing.Optional[typing.List[typing.Optional[bool]]]:
        field = self._get_field(field_name)
        kind = field.kind
        if kind == FieldKind.ARRAY_OF_BOOLEAN:
            # Optional[List[bool]] is compatible with Optional[List[Optional[bool]]]
            return self._read_var_sized_field(field, self._read_boolean_bits)  # type: ignore[return-value]
        elif kind == FieldKind.ARRAY_OF_NULLABLE_BOOLEAN:
            return self._read_var_sized_item_array(field, self._inp.read_boolean)
        else:
            raise DefaultCompactReader._create_mismatched_field_kind_error(
                field_name, kind, FieldKind.ARRAY_OF_NULLABLE_BOOLEAN
            )

    def read_array_of_nullable_boolean_or_default(
        self, field_name: str, default: typing.Optional[typing.List[typing.Optional[bool]]]
    ) -> typing.Optional[typing.List[typing.Optional[bool]]]:
        if not self._is_field_exists(field_name, FieldKind.ARRAY_OF_NULLABLE_BOOLEAN):
            return default

        return self.read_array_of_nullable_boolean(field_name)

    def read_array_of_int8(self, field_name: str) -> typing.Optional[typing.List[int]]:
        return self._read_fix_sized_item_array(
            field_name,
            FieldKind.ARRAY_OF_INT8,
            FieldKind.ARRAY_OF_NULLABLE_INT8,
            self._inp.read_i8_array,
        )

    def read_array_of_int8_or_default(
        self, field_name: str, default: typing.Optional[typing.List[int]]
    ) -> typing.Optional[typing.List[int]]:
        if not self._is_field_exists(field_name, FieldKind.ARRAY_OF_INT8):
            return default

        return self.read_array_of_int8(field_name)

    def read_array_of_nullable_int8(
        self, field_name: str
    ) -> typing.Optional[typing.List[typing.Optional[int]]]:
        return self._read_nullable_item_array(
            field_name,
            FieldKind.ARRAY_OF_INT8,
            FieldKind.ARRAY_OF_NULLABLE_INT8,
            self._inp.read_i8_array,
            self._inp.read_byte,
        )

    def read_array_of_nullable_int8_or_default(
        self, field_name: str, default: typing.Optional[typing.List[typing.Optional[int]]]
    ) -> typing.Optional[typing.List[typing.Optional[int]]]:
        if not self._is_field_exists(field_name, FieldKind.ARRAY_OF_NULLABLE_INT8):
            return default

        return self.read_array_of_nullable_int8(field_name)

    def read_array_of_int16(self, field_name: str) -> typing.Optional[typing.List[int]]:
        return self._read_fix_sized_item_array(
            field_name,
            FieldKind.ARRAY_OF_INT16,
            FieldKind.ARRAY_OF_NULLABLE_INT16,
            self._inp.read_short_array,
        )

    def read_array_of_int16_or_default(
        self, field_name: str, default: typing.Optional[typing.List[int]]
    ) -> typing.Optional[typing.List[int]]:
        if not self._is_field_exists(field_name, FieldKind.ARRAY_OF_INT16):
            return default

        return self.read_array_of_int16(field_name)

    def read_array_of_nullable_int16(
        self, field_name: str
    ) -> typing.Optional[typing.List[typing.Optional[int]]]:
        return self._read_nullable_item_array(
            field_name,
            FieldKind.ARRAY_OF_INT16,
            FieldKind.ARRAY_OF_NULLABLE_INT16,
            self._inp.read_short_array,
            self._inp.read_short,
        )

    def read_array_of_nullable_int16_or_default(
        self, field_name: str, default: typing.Optional[typing.List[typing.Optional[int]]]
    ) -> typing.Optional[typing.List[typing.Optional[int]]]:
        if not self._is_field_exists(field_name, FieldKind.ARRAY_OF_NULLABLE_INT16):
            return default

        return self.read_array_of_nullable_int16(field_name)

    def read_array_of_int32(self, field_name: str) -> typing.Optional[typing.List[int]]:
        return self._read_fix_sized_item_array(
            field_name,
            FieldKind.ARRAY_OF_INT32,
            FieldKind.ARRAY_OF_NULLABLE_INT32,
            self._inp.read_int_array,
        )

    def read_array_of_int32_or_default(
        self, field_name: str, default: typing.Optional[typing.List[int]]
    ) -> typing.Optional[typing.List[int]]:
        if not self._is_field_exists(field_name, FieldKind.ARRAY_OF_INT32):
            return default

        return self.read_array_of_int32(field_name)

    def read_array_of_nullable_int32(
        self, field_name: str
    ) -> typing.Optional[typing.List[typing.Optional[int]]]:
        return self._read_nullable_item_array(
            field_name,
            FieldKind.ARRAY_OF_INT32,
            FieldKind.ARRAY_OF_NULLABLE_INT32,
            self._inp.read_int_array,
            self._inp.read_int,
        )

    def read_array_of_nullable_int32_or_default(
        self, field_name: str, default: typing.Optional[typing.List[typing.Optional[int]]]
    ) -> typing.Optional[typing.List[typing.Optional[int]]]:
        if not self._is_field_exists(field_name, FieldKind.ARRAY_OF_NULLABLE_INT32):
            return default

        return self.read_array_of_nullable_int32(field_name)

    def read_array_of_int64(self, field_name: str) -> typing.Optional[typing.List[int]]:
        return self._read_fix_sized_item_array(
            field_name,
            FieldKind.ARRAY_OF_INT64,
            FieldKind.ARRAY_OF_NULLABLE_INT64,
            self._inp.read_long_array,
        )

    def read_array_of_int64_or_default(
        self, field_name: str, default: typing.Optional[typing.List[int]]
    ) -> typing.Optional[typing.List[int]]:
        if not self._is_field_exists(field_name, FieldKind.ARRAY_OF_INT64):
            return default

        return self.read_array_of_int64(field_name)

    def read_array_of_nullable_int64(
        self, field_name: str
    ) -> typing.Optional[typing.List[typing.Optional[int]]]:
        return self._read_nullable_item_array(
            field_name,
            FieldKind.ARRAY_OF_INT64,
            FieldKind.ARRAY_OF_NULLABLE_INT64,
            self._inp.read_long_array,
            self._inp.read_long,
        )

    def read_array_of_nullable_int64_or_default(
        self, field_name: str, default: typing.Optional[typing.List[typing.Optional[int]]]
    ) -> typing.Optional[typing.List[typing.Optional[int]]]:
        if not self._is_field_exists(field_name, FieldKind.ARRAY_OF_NULLABLE_INT64):
            return default

        return self.read_array_of_nullable_int64(field_name)

    def read_array_of_float32(self, field_name: str) -> typing.Optional[typing.List[float]]:
        return self._read_fix_sized_item_array(
            field_name,
            FieldKind.ARRAY_OF_FLOAT32,
            FieldKind.ARRAY_OF_NULLABLE_FLOAT32,
            self._inp.read_float_array,
        )

    def read_array_of_float32_or_default(
        self, field_name: str, default: typing.Optional[typing.List[float]]
    ) -> typing.Optional[typing.List[float]]:
        if not self._is_field_exists(field_name, FieldKind.ARRAY_OF_FLOAT32):
            return default

        return self.read_array_of_float32(field_name)

    def read_array_of_nullable_float32(
        self, field_name: str
    ) -> typing.Optional[typing.List[typing.Optional[float]]]:
        return self._read_nullable_item_array(
            field_name,
            FieldKind.ARRAY_OF_FLOAT32,
            FieldKind.ARRAY_OF_NULLABLE_FLOAT32,
            self._inp.read_float_array,
            self._inp.read_float,
        )

    def read_array_of_nullable_float32_or_default(
        self, field_name: str, default: typing.Optional[typing.List[typing.Optional[float]]]
    ) -> typing.Optional[typing.List[typing.Optional[float]]]:
        if not self._is_field_exists(field_name, FieldKind.ARRAY_OF_NULLABLE_FLOAT32):
            return default

        return self.read_array_of_nullable_float32(field_name)

    def read_array_of_float64(self, field_name: str) -> typing.Optional[typing.List[float]]:
        return self._read_fix_sized_item_array(
            field_name,
            FieldKind.ARRAY_OF_FLOAT64,
            FieldKind.ARRAY_OF_NULLABLE_FLOAT64,
            self._inp.read_double_array,
        )

    def read_array_of_float64_or_default(
        self, field_name: str, default: typing.Optional[typing.List[float]]
    ) -> typing.Optional[typing.List[float]]:
        if not self._is_field_exists(field_name, FieldKind.ARRAY_OF_FLOAT64):
            return default

        return self.read_array_of_float64(field_name)

    def read_array_of_nullable_float64(
        self, field_name: str
    ) -> typing.Optional[typing.List[typing.Optional[float]]]:
        return self._read_nullable_item_array(
            field_name,
            FieldKind.ARRAY_OF_FLOAT64,
            FieldKind.ARRAY_OF_NULLABLE_FLOAT64,
            self._inp.read_double_array,
            self._inp.read_double,
        )

    def read_array_of_nullable_float64_or_default(
        self, field_name: str, default: typing.Optional[typing.List[typing.Optional[float]]]
    ) -> typing.Optional[typing.List[typing.Optional[float]]]:
        if not self._is_field_exists(field_name, FieldKind.ARRAY_OF_NULLABLE_FLOAT64):
            return default

        return self.read_array_of_nullable_float64(field_name)

    def read_array_of_string(
        self, field_name: str
    ) -> typing.Optional[typing.List[typing.Optional[str]]]:
        field = self._get_field_with_kind_check(field_name, FieldKind.ARRAY_OF_STRING)
        return self._read_var_sized_item_array(field, self._inp.read_string)

    def read_array_of_string_or_default(
        self, field_name: str, default: typing.Optional[typing.List[typing.Optional[str]]]
    ) -> typing.Optional[typing.List[typing.Optional[str]]]:
        if not self._is_field_exists(field_name, FieldKind.ARRAY_OF_STRING):
            return default

        return self.read_array_of_string(field_name)

    def read_array_of_decimal(
        self, field_name: str
    ) -> typing.Optional[typing.List[typing.Optional[decimal.Decimal]]]:
        field = self._get_field_with_kind_check(field_name, FieldKind.ARRAY_OF_DECIMAL)
        return self._read_var_sized_item_array(field, self._read_decimal_helper)

    def read_array_of_decimal_or_default(
        self,
        field_name: str,
        default: typing.Optional[typing.List[typing.Optional[decimal.Decimal]]],
    ) -> typing.Optional[typing.List[typing.Optional[decimal.Decimal]]]:
        if not self._is_field_exists(field_name, FieldKind.ARRAY_OF_DECIMAL):
            return default

        return self.read_array_of_decimal(field_name)

    def read_array_of_time(
        self, field_name: str
    ) -> typing.Optional[typing.List[typing.Optional[datetime.time]]]:
        field = self._get_field_with_kind_check(field_name, FieldKind.ARRAY_OF_TIME)
        return self._read_var_sized_item_array(field, self._read_time_helper)

    def read_array_of_time_or_default(
        self, field_name: str, default: typing.Optional[typing.List[typing.Optional[datetime.time]]]
    ) -> typing.Optional[typing.List[typing.Optional[datetime.time]]]:
        if not self._is_field_exists(field_name, FieldKind.ARRAY_OF_TIME):
            return default

        return self.read_array_of_time(field_name)

    def read_array_of_date(
        self, field_name: str
    ) -> typing.Optional[typing.List[typing.Optional[datetime.date]]]:
        field = self._get_field_with_kind_check(field_name, FieldKind.ARRAY_OF_DATE)
        return self._read_var_sized_item_array(field, self._read_date_helper)

    def read_array_of_date_or_default(
        self, field_name: str, default: typing.Optional[typing.List[typing.Optional[datetime.date]]]
    ) -> typing.Optional[typing.List[typing.Optional[datetime.date]]]:
        if not self._is_field_exists(field_name, FieldKind.ARRAY_OF_DATE):
            return default

        return self.read_array_of_date(field_name)

    def read_array_of_timestamp(
        self, field_name: str
    ) -> typing.Optional[typing.List[typing.Optional[datetime.datetime]]]:
        field = self._get_field_with_kind_check(field_name, FieldKind.ARRAY_OF_TIMESTAMP)
        return self._read_var_sized_item_array(field, self._read_timestamp_helper)

    def read_array_of_timestamp_or_default(
        self,
        field_name: str,
        default: typing.Optional[typing.List[typing.Optional[datetime.datetime]]],
    ) -> typing.Optional[typing.List[typing.Optional[datetime.datetime]]]:
        if not self._is_field_exists(field_name, FieldKind.ARRAY_OF_TIMESTAMP):
            return default

        return self.read_array_of_timestamp(field_name)

    def read_array_of_timestamp_with_timezone(
        self, field_name: str
    ) -> typing.Optional[typing.List[typing.Optional[datetime.datetime]]]:
        field = self._get_field_with_kind_check(
            field_name, FieldKind.ARRAY_OF_TIMESTAMP_WITH_TIMEZONE
        )
        return self._read_var_sized_item_array(field, self._read_timestamp_with_timezone_helper)

    def read_array_of_timestamp_with_timezone_or_default(
        self,
        field_name: str,
        default: typing.Optional[typing.List[typing.Optional[datetime.datetime]]],
    ) -> typing.Optional[typing.List[typing.Optional[datetime.datetime]]]:
        if not self._is_field_exists(field_name, FieldKind.ARRAY_OF_TIMESTAMP_WITH_TIMEZONE):
            return default

        return self.read_array_of_timestamp_with_timezone(field_name)

    def read_array_of_compact(
        self, field_name: str
    ) -> typing.Optional[typing.List[typing.Optional[typing.Any]]]:
        field = self._get_field_with_kind_check(field_name, FieldKind.ARRAY_OF_COMPACT)
        return self._read_var_sized_item_array(field, self._read_compact_helper)

    def read_array_of_compact_or_default(
        self, field_name: str, default: typing.Optional[typing.List[typing.Optional[typing.Any]]]
    ) -> typing.Optional[typing.List[typing.Optional[typing.Any]]]:
        if not self._is_field_exists(field_name, FieldKind.ARRAY_OF_COMPACT):
            return default

        return self.read_array_of_compact(field_name)

    def _get_field(self, field_name: str) -> "FieldDescriptor":
        field = self._schema.fields.get(field_name)
        if not field:
            raise HazelcastSerializationError(
                f"No field with the name '{field_name}' can be found in the {self._schema}"
            )

        return field

    def _get_field_with_kind_check(
        self, field_name: str, field_kind: "FieldKind"
    ) -> "FieldDescriptor":
        field = self._get_field(field_name)
        if field.kind != field_kind:
            raise DefaultCompactReader._create_mismatched_field_kind_error(
                field_name, field.kind, field_kind
            )

        return field

    def _read_boolean_internal(self, field: "FieldDescriptor") -> bool:
        position = self._data_start_position + field.position
        bit_position = field.bit_position
        packed_byte = self._inp.read_byte_positional(position)
        return ((packed_byte >> bit_position) & 0x01) != 0

    @staticmethod
    def _create_mismatched_field_kind_error(
        field_name: str, field_kind: "FieldKind", expected_field_kind: "FieldKind"
    ) -> HazelcastSerializationError:
        raise HazelcastSerializationError(
            f"Mismatched field types. "
            f"Expected: {expected_field_kind}, found: {field_kind} for the '{field_name}'."
        )

    def _read_var_sized_field_non_none(
        self, field: "FieldDescriptor", reader: typing.Callable[[], T]
    ) -> T:
        value = self._read_var_sized_field(field, reader)
        if value is None:
            raise HazelcastSerializationError(
                f"Error while reading '{field.name}'."
                f"A 'None' value cannot be read with 'read_{field.kind.name.lower()}' method."
                f"Use 'read_nullable_{field.kind.name.lower()}' method instead."
            )

        return value

    def _read_var_sized_field(
        self, field: "FieldDescriptor", reader: typing.Callable[[], T]
    ) -> typing.Optional[T]:
        current_position = self._inp.position()
        try:
            position = self._read_var_sized_field_position(field)
            if position == PositionReader.NULL_POSITION:
                return None

            self._inp.set_position(position)
            return reader()
        finally:
            self._inp.set_position(current_position)

    def _read_var_sized_field_position(self, field: "FieldDescriptor") -> int:
        index = field.index
        position = self._position_reader.read(
            self._inp, self._var_sized_field_positions_position, index
        )
        if position == PositionReader.NULL_POSITION:
            return PositionReader.NULL_POSITION
        return position + self._data_start_position

    def _is_field_exists(self, field_name: str, field_kind: "FieldKind") -> bool:
        field = self._schema.fields.get(field_name)
        if not field:
            return False

        return field_kind == field.kind

    def _read_boolean_bits(self) -> typing.List[bool]:
        inp = self._inp
        n = inp.read_int()
        values: typing.List[bool] = []
        if n == 0:
            return values

        bit_position = 0
        current_byte = inp.read_byte()
        for _ in range(n):
            if bit_position == _BOOLEANS_PER_BYTE:
                bit_position = 0
                current_byte = inp.read_byte()

            value = ((current_byte >> bit_position) & 0x01) != 0
            bit_position += 1
            values.append(value)

        return values

    def _read_nullable_item_array_as_fix_sized_item_array(
        self, field: "FieldDescriptor", reader: typing.Callable[[], typing.List[T]]
    ) -> typing.Optional[typing.List[T]]:
        inp = self._inp
        current_position = inp.position()
        try:
            position = self._read_var_sized_field_position(field)
            if position == PositionReader.NULL_POSITION:
                return None

            inp.set_position(position)

            # Verify that no item is None by checking their positions,
            # and then read.

            data_length = inp.read_int()
            item_count = inp.read_int()
            data_start_position = inp.position()

            position_reader = PositionReader.reader_for(data_length)
            item_positions_position = data_start_position + data_length
            for i in range(item_count):
                item_position = position_reader.read(inp, item_positions_position, i)
                if item_position == PositionReader.NULL_POSITION:
                    self._raise_on_none_value_in_fix_sized_item_array(field)

            inp.set_position(data_start_position - INT_SIZE_IN_BYTES)
            return reader()
        finally:
            inp.set_position(current_position)

    def _read_var_sized_item_array(
        self, field: "FieldDescriptor", item_reader: typing.Callable[[], T]
    ) -> typing.Optional[typing.List[typing.Optional[T]]]:
        inp = self._inp
        current_position = inp.position()
        try:
            position = self._read_var_sized_field_position(field)
            if position == PositionReader.NULL_POSITION:
                return None

            inp.set_position(position)

            data_length = inp.read_int()
            item_count = inp.read_int()
            data_start_position = inp.position()

            values: typing.List[typing.Optional[T]] = [None] * item_count
            position_reader = PositionReader.reader_for(data_length)
            item_positions_position = data_start_position + data_length
            for i in range(item_count):
                item_position = position_reader.read(inp, item_positions_position, i)
                if item_position != PositionReader.NULL_POSITION:
                    inp.set_position(data_start_position + item_position)
                    values[i] = item_reader()

            return values
        finally:
            inp.set_position(current_position)

    def _read_fix_sized_item_array(
        self,
        field_name: str,
        fix_sized_item_array_field_kind: "FieldKind",
        nullable_fix_sized_item_array_field_kind: "FieldKind",
        reader: typing.Callable[[], typing.List[T]],
    ) -> typing.Optional[typing.List[T]]:
        field = self._get_field(field_name)
        kind = field.kind
        if kind == fix_sized_item_array_field_kind:
            return self._read_var_sized_field(field, reader)
        elif kind == nullable_fix_sized_item_array_field_kind:
            return self._read_nullable_item_array_as_fix_sized_item_array(field, reader)
        else:
            raise DefaultCompactReader._create_mismatched_field_kind_error(
                field_name, kind, fix_sized_item_array_field_kind
            )

    def _read_nullable_item_array(
        self,
        field_name: str,
        fix_sized_item_array_field_kind: "FieldKind",
        nullable_fix_sized_item_array_field_kind: "FieldKind",
        fix_sized_item_array_reader: typing.Callable[[], typing.List[T]],
        item_reader: typing.Callable[[], T],
    ) -> typing.Optional[typing.List[typing.Optional[T]]]:
        field = self._get_field(field_name)
        kind = field.kind
        if kind == fix_sized_item_array_field_kind:
            # Optional[List[T]] is compatible with Optional[List[Optional[T]]]
            return self._read_var_sized_field(field, fix_sized_item_array_reader)  # type: ignore[return-value]
        elif kind == nullable_fix_sized_item_array_field_kind:
            return self._read_var_sized_item_array(field, item_reader)
        else:
            raise DefaultCompactReader._create_mismatched_field_kind_error(
                field_name, kind, fix_sized_item_array_field_kind
            )

    def _read_decimal_helper(self) -> decimal.Decimal:
        return IOUtil.read_big_decimal(self._inp)

    def _read_time_helper(self) -> datetime.time:
        return IOUtil.read_time(self._inp)

    def _read_date_helper(self) -> datetime.date:
        return IOUtil.read_date(self._inp)

    def _read_timestamp_helper(self) -> datetime.datetime:
        return IOUtil.read_timestamp(self._inp)

    def _read_timestamp_with_timezone_helper(self) -> datetime.datetime:
        return IOUtil.read_timestamp_with_timezone(self._inp)

    def _read_compact_helper(self) -> typing.Any:
        return self._compact_serializer.read(self._inp)

    @staticmethod
    def _raise_on_none_value_in_fix_sized_item_array(field: "FieldDescriptor") -> typing.NoReturn:
        method_suffix = field.kind.name.lower()
        nullable_method_suffix_parts = method_suffix.split("_", 2)  # array_of_something
        nullable_method_suffix_parts.insert(2, "nullable")
        nullable_method_suffix = "_".join(nullable_method_suffix_parts)
        raise HazelcastSerializationError(
            f"Error while reading '{field.name}'."
            f"A `None` item cannot be read with `read_{method_suffix}` method."
            f"Use 'read_{nullable_method_suffix}' method instead."
        )


class SchemaWriter(CompactWriter):
    def __init__(self, type_name: str):
        self._type_name = type_name
        self._fields: typing.List[FieldDescriptor] = []

    def build(self) -> "Schema":
        return Schema(self._type_name, self._fields)

    def write_boolean(self, field_name: str, value: bool) -> None:
        self._add_field(field_name, FieldKind.BOOLEAN)

    def write_nullable_boolean(self, field_name: str, value: typing.Optional[bool]) -> None:
        self._add_field(field_name, FieldKind.NULLABLE_BOOLEAN)

    def write_int8(self, field_name: str, value: int) -> None:
        self._add_field(field_name, FieldKind.INT8)

    def write_nullable_int8(self, field_name: str, value: typing.Optional[int]) -> None:
        self._add_field(field_name, FieldKind.NULLABLE_INT8)

    def write_int16(self, field_name: str, value: int) -> None:
        self._add_field(field_name, FieldKind.INT16)

    def write_nullable_int16(self, field_name: str, value: typing.Optional[int]) -> None:
        self._add_field(field_name, FieldKind.NULLABLE_INT16)

    def write_int32(self, field_name: str, value: int) -> None:
        self._add_field(field_name, FieldKind.INT32)

    def write_nullable_int32(self, field_name: str, value: typing.Optional[int]) -> None:
        self._add_field(field_name, FieldKind.NULLABLE_INT32)

    def write_int64(self, field_name: str, value: int) -> None:
        self._add_field(field_name, FieldKind.INT64)

    def write_nullable_int64(self, field_name: str, value: typing.Optional[int]) -> None:
        self._add_field(field_name, FieldKind.NULLABLE_INT64)

    def write_float32(self, field_name: str, value: float) -> None:
        self._add_field(field_name, FieldKind.FLOAT32)

    def write_nullable_float32(self, field_name: str, value: typing.Optional[float]) -> None:
        self._add_field(field_name, FieldKind.NULLABLE_FLOAT32)

    def write_float64(self, field_name: str, value: float) -> None:
        self._add_field(field_name, FieldKind.FLOAT64)

    def write_nullable_float64(self, field_name: str, value: typing.Optional[float]) -> None:
        self._add_field(field_name, FieldKind.NULLABLE_FLOAT64)

    def write_string(self, field_name: str, value: typing.Optional[str]) -> None:
        self._add_field(field_name, FieldKind.STRING)

    def write_decimal(self, field_name: str, value: typing.Optional[decimal.Decimal]) -> None:
        self._add_field(field_name, FieldKind.DECIMAL)

    def write_time(self, field_name: str, value: typing.Optional[datetime.time]) -> None:
        self._add_field(field_name, FieldKind.TIME)

    def write_date(self, field_name: str, value: typing.Optional[datetime.date]) -> None:
        self._add_field(field_name, FieldKind.DATE)

    def write_timestamp(self, field_name: str, value: typing.Optional[datetime.datetime]) -> None:
        self._add_field(field_name, FieldKind.TIMESTAMP)

    def write_timestamp_with_timezone(
        self, field_name: str, value: typing.Optional[datetime.datetime]
    ) -> None:
        self._add_field(field_name, FieldKind.TIMESTAMP_WITH_TIMEZONE)

    def write_compact(self, field_name: str, value: typing.Optional[typing.Any]) -> None:
        self._add_field(field_name, FieldKind.COMPACT)

    def write_array_of_boolean(
        self, field_name: str, value: typing.Optional[typing.List[bool]]
    ) -> None:
        self._add_field(field_name, FieldKind.ARRAY_OF_BOOLEAN)

    def write_array_of_nullable_boolean(
        self, field_name: str, value: typing.Optional[typing.List[typing.Optional[bool]]]
    ) -> None:
        self._add_field(field_name, FieldKind.ARRAY_OF_NULLABLE_BOOLEAN)

    def write_array_of_int8(
        self, field_name: str, value: typing.Optional[typing.List[int]]
    ) -> None:
        self._add_field(field_name, FieldKind.ARRAY_OF_INT8)

    def write_array_of_nullable_int8(
        self, field_name: str, value: typing.Optional[typing.List[typing.Optional[int]]]
    ) -> None:
        self._add_field(field_name, FieldKind.ARRAY_OF_NULLABLE_INT8)

    def write_array_of_int16(
        self, field_name: str, value: typing.Optional[typing.List[int]]
    ) -> None:
        self._add_field(field_name, FieldKind.ARRAY_OF_INT16)

    def write_array_of_nullable_int16(
        self, field_name: str, value: typing.Optional[typing.List[typing.Optional[int]]]
    ) -> None:
        self._add_field(field_name, FieldKind.ARRAY_OF_NULLABLE_INT16)

    def write_array_of_int32(
        self, field_name: str, value: typing.Optional[typing.List[int]]
    ) -> None:
        self._add_field(field_name, FieldKind.ARRAY_OF_INT32)

    def write_array_of_nullable_int32(
        self, field_name: str, value: typing.Optional[typing.List[typing.Optional[int]]]
    ) -> None:
        self._add_field(field_name, FieldKind.ARRAY_OF_NULLABLE_INT32)

    def write_array_of_int64(
        self, field_name: str, value: typing.Optional[typing.List[int]]
    ) -> None:
        self._add_field(field_name, FieldKind.ARRAY_OF_INT64)

    def write_array_of_nullable_int64(
        self, field_name: str, value: typing.Optional[typing.List[typing.Optional[int]]]
    ) -> None:
        self._add_field(field_name, FieldKind.ARRAY_OF_NULLABLE_INT64)

    def write_array_of_float32(
        self, field_name: str, value: typing.Optional[typing.List[float]]
    ) -> None:
        self._add_field(field_name, FieldKind.ARRAY_OF_FLOAT32)

    def write_array_of_nullable_float32(
        self, field_name: str, value: typing.Optional[typing.List[typing.Optional[float]]]
    ) -> None:
        self._add_field(field_name, FieldKind.ARRAY_OF_NULLABLE_FLOAT32)

    def write_array_of_float64(
        self, field_name: str, value: typing.Optional[typing.List[float]]
    ) -> None:
        self._add_field(field_name, FieldKind.ARRAY_OF_FLOAT64)

    def write_array_of_nullable_float64(
        self, field_name: str, value: typing.Optional[typing.List[typing.Optional[float]]]
    ) -> None:
        self._add_field(field_name, FieldKind.ARRAY_OF_NULLABLE_FLOAT64)

    def write_array_of_string(
        self, field_name: str, value: typing.Optional[typing.List[typing.Optional[str]]]
    ) -> None:
        self._add_field(field_name, FieldKind.ARRAY_OF_STRING)

    def write_array_of_decimal(
        self, field_name: str, value: typing.Optional[typing.List[typing.Optional[decimal.Decimal]]]
    ) -> None:
        self._add_field(field_name, FieldKind.ARRAY_OF_DECIMAL)

    def write_array_of_time(
        self, field_name: str, value: typing.Optional[typing.List[typing.Optional[datetime.time]]]
    ) -> None:
        self._add_field(field_name, FieldKind.ARRAY_OF_TIME)

    def write_array_of_date(
        self, field_name: str, value: typing.Optional[typing.List[typing.Optional[datetime.date]]]
    ) -> None:
        self._add_field(field_name, FieldKind.ARRAY_OF_DATE)

    def write_array_of_timestamp(
        self,
        field_name: str,
        value: typing.Optional[typing.List[typing.Optional[datetime.datetime]]],
    ) -> None:
        self._add_field(field_name, FieldKind.ARRAY_OF_TIMESTAMP)

    def write_array_of_timestamp_with_timezone(
        self,
        field_name: str,
        value: typing.Optional[typing.List[typing.Optional[datetime.datetime]]],
    ) -> None:
        self._add_field(field_name, FieldKind.ARRAY_OF_TIMESTAMP_WITH_TIMEZONE)

    def write_array_of_compact(
        self, field_name: str, value: typing.Optional[typing.List[typing.Optional[typing.Any]]]
    ) -> None:
        self._add_field(field_name, FieldKind.ARRAY_OF_COMPACT)

    def _add_field(self, name: str, kind: "FieldKind"):
        self._fields.append(FieldDescriptor(name, kind))


class Schema:
    def __init__(self, type_name: str, fields_list: typing.List["FieldDescriptor"]):
        self.type_name = type_name
        self.fields: typing.Dict[str, "FieldDescriptor"] = Schema._dict_to_key_ordered_dict(
            {f.name: f for f in fields_list}
        )
        self.fields_list = list(self.fields.values())
        self.schema_id: int = 0
        self.var_sized_field_count: int = 0
        self.fix_sized_fields_length: int = 0
        self._init()

    def _init(self):
        fix_sized_fields = []
        var_sized_fields = []
        bool_fields = []

        for field in self.fields_list:
            kind = field.kind
            if kind < 0 or kind >= FieldKind.NOT_AVAILABLE:
                raise HazelcastSerializationError(f"Invalid field kind: {kind}")
            if FIELD_OPERATIONS[field.kind].is_var_sized():
                var_sized_fields.append(field)
            elif FieldKind.BOOLEAN == kind:
                bool_fields.append(field)
            else:
                fix_sized_fields.append(field)

        fix_sized_fields.sort(
            key=lambda f: FIELD_OPERATIONS[f.kind].size_in_bytes(),
            reverse=True,
        )

        position = 0
        for field in fix_sized_fields:
            field.position = position
            position += FIELD_OPERATIONS[field.kind].size_in_bytes()

        bit_position = 0
        for field in bool_fields:
            field.position = position
            field.bit_position = bit_position % _BOOLEANS_PER_BYTE
            bit_position += 1
            if bit_position % _BOOLEANS_PER_BYTE == 0:
                position += 1

        if bit_position % _BOOLEANS_PER_BYTE != 0:
            position += 1

        self.fix_sized_fields_length = position

        index = 0
        for field in var_sized_fields:
            field.index = index
            index += 1

        self.var_sized_field_count = index
        self.schema_id = RabinFingerprint.of(self)

    @staticmethod
    def _dict_to_key_ordered_dict(
        d: typing.Dict[str, "FieldDescriptor"]
    ) -> typing.Dict[str, "FieldDescriptor"]:
        od = collections.OrderedDict()
        for key in sorted(d):
            od[key] = d[key]
        return od

    def __eq__(self, other: typing.Any) -> bool:
        return (
            isinstance(other, Schema)
            and self.type_name == other.type_name
            and self.fields == other.fields
            and self.fields_list == other.fields_list
            and self.schema_id == other.schema_id
            and self.var_sized_field_count == other.var_sized_field_count
            and self.fix_sized_fields_length == other.fix_sized_fields_length
        )

    def __repr__(self) -> str:
        return (
            f"Schema(schema_id={self.schema_id}, type_name={self.type_name}, fields={self.fields})"
        )


class FieldDescriptor:
    def __init__(self, name: str, kind: "FieldKind"):
        self.name = name
        if not isinstance(kind, FieldKind):
            kind = FieldKind(kind)
        self.kind = kind
        self.index = -1
        self.position = -1
        self.bit_position = -1

    def __eq__(self, other: typing.Any) -> bool:
        return (
            isinstance(other, FieldDescriptor)
            and self.name == other.name
            and self.kind == other.kind
        )

    def __repr__(self) -> str:
        return (
            f"FieldDescriptor(name={self.name}, kind={self.kind}, "
            f"index={self.index}, position={self.position}, "
            f"bit_position={self.bit_position})"
        )


def _init_fingerprint_table() -> typing.Tuple[int, typing.List[int]]:
    empty = -4513414715797952619
    table_size = 256
    table = [0] * table_size
    for i in range(table_size):
        fp = i
        for _ in range(8):
            fp = ((fp % 0x10000000000000000) >> 1) ^ (empty & -(fp & 1))
        table[i] = fp
    return empty, table


class RabinFingerprint:
    _EMPTY, _TABLE = _init_fingerprint_table()

    @staticmethod
    def of(schema: Schema) -> int:
        fp = RabinFingerprint._of_str(RabinFingerprint._EMPTY, schema.type_name)
        fp = RabinFingerprint._of_i32(fp, len(schema.fields))
        for field in schema.fields_list:
            fp = RabinFingerprint._of_str(fp, field.name)
            fp = RabinFingerprint._of_i32(fp, field.kind)
        return fp

    @staticmethod
    def _of_str(fp: int, value: str) -> int:
        utf8_bytes = value.encode("utf-8")
        fp = RabinFingerprint._of_i32(fp, len(utf8_bytes))
        for b in utf8_bytes:
            fp = RabinFingerprint._of_i8(fp, b)
        return fp

    @staticmethod
    def _of_i32(fp: int, value: int) -> int:
        fp = RabinFingerprint._of_i8(fp, value & 0xFF)
        fp = RabinFingerprint._of_i8(
            fp, RabinFingerprint._logical_right_shift_32_bit(value, 8) & 0xFF
        )
        fp = RabinFingerprint._of_i8(
            fp, RabinFingerprint._logical_right_shift_32_bit(value, 16) & 0xFF
        )
        fp = RabinFingerprint._of_i8(
            fp, RabinFingerprint._logical_right_shift_32_bit(value, 24) & 0xFF
        )
        return fp

    @staticmethod
    def _of_i8(fp: int, value: int) -> int:
        return (
            RabinFingerprint._logical_right_shift_64_bit(fp, 8)
            ^ RabinFingerprint._TABLE[(fp ^ value) & 0xFF]
        )

    @staticmethod
    def _logical_right_shift_64_bit(value: int, shift: int) -> int:
        if value >= 0:
            return value >> shift

        return (value & 0xFFFFFFFFFFFFFFFF) >> shift

    @staticmethod
    def _logical_right_shift_32_bit(value: int, shift: int) -> int:
        if value >= 0:
            return value >> shift

        return (value & 0xFFFFFFFF) >> shift


class SchemaNotReplicatedError(HazelcastError):
    def __init__(self, schema: Schema, clazz: typing.Type):
        super(SchemaNotReplicatedError, self).__init__()
        self.schema = schema
        self.clazz = clazz


class SchemaNotFoundError(HazelcastError):
    def __init__(self, schema_id: int):
        super(SchemaNotFoundError, self).__init__()
        self.schema_id = schema_id


class PositionReader(abc.ABC):
    """Reads the positions of the variable-size fields with the given indexes."""

    NULL_POSITION = -1
    """Position of the null fields."""

    UINT8_POSITION_READER_RANGE = MAX_BYTE - MIN_BYTE
    """Range of the positions that can be represented by a single byte and can
    be read with :class:`UInt8PositionReader`.
    """

    UINT16_POSITION_READER_RANGE = MAX_SHORT - MIN_SHORT
    """Range of the positions that can be represented by two bytes and can be
    read with :class:`UInt16PositionReader`.
    """

    @abc.abstractmethod
    def read(self, inp: _ObjectDataInput, start_position: int, index: int) -> int:
        """Returns the position of the variable-size field at the given index.

        Args:
            inp: Input to read the position from.
            start_position: Start of the variable-size field positions section of the
                input.
            index: Index of the field.

        Returns:
            The position.
        """

    @staticmethod
    def reader_for(data_length) -> "PositionReader":
        if data_length < PositionReader.UINT8_POSITION_READER_RANGE:
            return _UINT8_POSITION_READER_INSTANCE
        elif data_length < PositionReader.UINT16_POSITION_READER_RANGE:
            return _UINT16_POSITION_READER_INSTANCE
        else:
            return _INT32_POSITION_READER_INSTANCE


class UInt8PositionReader(PositionReader):
    def read(self, inp: _ObjectDataInput, start_position: int, index: int) -> int:
        position = inp.read_byte_positional(start_position + index)
        if position == PositionReader.NULL_POSITION:
            return position
        return position & 0xFF


_UINT8_POSITION_READER_INSTANCE = UInt8PositionReader()


class UInt16PositionReader(PositionReader):
    def read(self, inp: _ObjectDataInput, start_position: int, index: int) -> int:
        position = inp.read_short_positional(start_position + index * SHORT_SIZE_IN_BYTES)
        if position == PositionReader.NULL_POSITION:
            return position
        return position & 0xFFFF


_UINT16_POSITION_READER_INSTANCE = UInt16PositionReader()


class Int32PositionReader(PositionReader):
    def read(self, inp: _ObjectDataInput, start_position: int, index: int) -> int:
        return inp.read_int_positional(start_position + index * INT_SIZE_IN_BYTES)


_INT32_POSITION_READER_INSTANCE = Int32PositionReader()


class FieldKind(enum.IntEnum):
    BOOLEAN = 0
    ARRAY_OF_BOOLEAN = 1
    INT8 = 2
    ARRAY_OF_INT8 = 3
    INT16 = 6
    ARRAY_OF_INT16 = 7
    INT32 = 8
    ARRAY_OF_INT32 = 9
    INT64 = 10
    ARRAY_OF_INT64 = 11
    FLOAT32 = 12
    ARRAY_OF_FLOAT32 = 13
    FLOAT64 = 14
    ARRAY_OF_FLOAT64 = 15
    STRING = 16
    ARRAY_OF_STRING = 17
    DECIMAL = 18
    ARRAY_OF_DECIMAL = 19
    TIME = 20
    ARRAY_OF_TIME = 21
    DATE = 22
    ARRAY_OF_DATE = 23
    TIMESTAMP = 24
    ARRAY_OF_TIMESTAMP = 25
    TIMESTAMP_WITH_TIMEZONE = 26
    ARRAY_OF_TIMESTAMP_WITH_TIMEZONE = 27
    COMPACT = 28
    ARRAY_OF_COMPACT = 29
    NULLABLE_BOOLEAN = 32
    ARRAY_OF_NULLABLE_BOOLEAN = 33
    NULLABLE_INT8 = 34
    ARRAY_OF_NULLABLE_INT8 = 35
    NULLABLE_INT16 = 36
    ARRAY_OF_NULLABLE_INT16 = 37
    NULLABLE_INT32 = 38
    ARRAY_OF_NULLABLE_INT32 = 39
    NULLABLE_INT64 = 40
    ARRAY_OF_NULLABLE_INT64 = 41
    NULLABLE_FLOAT32 = 42
    ARRAY_OF_NULLABLE_FLOAT32 = 43
    NULLABLE_FLOAT64 = 44
    ARRAY_OF_NULLABLE_FLOAT64 = 45
    NOT_AVAILABLE = 46


class FieldKindOperations(abc.ABC):
    _VAR_SIZE = -1

    def size_in_bytes(self) -> int:
        return FieldKindOperations._VAR_SIZE

    def is_var_sized(self):
        return self.size_in_bytes() == FieldKindOperations._VAR_SIZE


class BooleanOperations(FieldKindOperations):
    def size_in_bytes(self) -> int:
        return 0


class ArrayOfBooleanOperations(FieldKindOperations):
    pass


class Int8Operations(FieldKindOperations):
    def size_in_bytes(self) -> int:
        return BYTE_SIZE_IN_BYTES


class ArrayOfInt8Operations(FieldKindOperations):
    pass


class Int16Operations(FieldKindOperations):
    def size_in_bytes(self) -> int:
        return SHORT_SIZE_IN_BYTES


class ArrayOfInt16Operations(FieldKindOperations):
    pass


class Int32Operations(FieldKindOperations):
    def size_in_bytes(self) -> int:
        return INT_SIZE_IN_BYTES


class ArrayOfInt32Operations(FieldKindOperations):
    pass


class Int64Operations(FieldKindOperations):
    def size_in_bytes(self) -> int:
        return LONG_SIZE_IN_BYTES


class ArrayOfInt64Operations(FieldKindOperations):
    pass


class Float32Operations(FieldKindOperations):
    def size_in_bytes(self) -> int:
        return FLOAT_SIZE_IN_BYTES


class ArrayOfFloat32Operations(FieldKindOperations):
    pass


class Float64Operations(FieldKindOperations):
    def size_in_bytes(self) -> int:
        return DOUBLE_SIZE_IN_BYTES


class ArrayOfFloat64Operations(FieldKindOperations):
    pass


class StringOperations(FieldKindOperations):
    pass


class ArrayOfStringOperations(FieldKindOperations):
    pass


class DecimalOperations(FieldKindOperations):
    pass


class ArrayOfDecimalOperations(FieldKindOperations):
    pass


class TimeOperations(FieldKindOperations):
    pass


class ArrayOfTimeOperations(FieldKindOperations):
    pass


class DateOperations(FieldKindOperations):
    pass


class ArrayOfDateOperations(FieldKindOperations):
    pass


class TimestampOperations(FieldKindOperations):
    pass


class ArrayOfTimestampOperations(FieldKindOperations):
    pass


class TimestampWithTimezoneOperations(FieldKindOperations):
    pass


class ArrayOfTimestampWithTimezoneOperations(FieldKindOperations):
    pass


class CompactOperations(FieldKindOperations):
    pass


class ArrayOfCompactOperations(FieldKindOperations):
    pass


class NullableBooleanOperations(FieldKindOperations):
    pass


class ArrayOfNullableBooleanOperations(FieldKindOperations):
    pass


class NullableInt8Operations(FieldKindOperations):
    pass


class ArrayOfNullableInt8Operations(FieldKindOperations):
    pass


class NullableInt16Operations(FieldKindOperations):
    pass


class ArrayOfNullableInt16Operations(FieldKindOperations):
    pass


class NullableInt32Operations(FieldKindOperations):
    pass


class ArrayOfNullableInt32Operations(FieldKindOperations):
    pass


class NullableInt64Operations(FieldKindOperations):
    pass


class ArrayOfNullableInt64Operations(FieldKindOperations):
    pass


class NullableFloat32Operations(FieldKindOperations):
    pass


class ArrayOfNullableFloat32Operations(FieldKindOperations):
    pass


class NullableFloat64Operations(FieldKindOperations):
    pass


class ArrayOfNullableFloat64Operations(FieldKindOperations):
    pass


FIELD_OPERATIONS: typing.List[typing.Optional[FieldKindOperations]] = [
    BooleanOperations(),
    ArrayOfBooleanOperations(),
    Int8Operations(),
    ArrayOfInt8Operations(),
    None,
    None,
    Int16Operations(),
    ArrayOfInt16Operations(),
    Int32Operations(),
    ArrayOfInt32Operations(),
    Int64Operations(),
    ArrayOfInt64Operations(),
    Float32Operations(),
    ArrayOfFloat32Operations(),
    Float64Operations(),
    ArrayOfFloat64Operations(),
    StringOperations(),
    ArrayOfStringOperations(),
    DecimalOperations(),
    ArrayOfDecimalOperations(),
    TimeOperations(),
    ArrayOfTimeOperations(),
    DateOperations(),
    ArrayOfDateOperations(),
    TimestampOperations(),
    ArrayOfTimestampOperations(),
    TimestampWithTimezoneOperations(),
    ArrayOfTimestampWithTimezoneOperations(),
    CompactOperations(),
    ArrayOfCompactOperations(),
    None,
    None,
    NullableBooleanOperations(),
    ArrayOfNullableBooleanOperations(),
    NullableInt8Operations(),
    ArrayOfNullableInt8Operations(),
    NullableInt16Operations(),
    ArrayOfNullableInt16Operations(),
    NullableInt32Operations(),
    ArrayOfNullableInt32Operations(),
    NullableInt64Operations(),
    ArrayOfNullableInt64Operations(),
    NullableFloat32Operations(),
    ArrayOfNullableFloat32Operations(),
    NullableFloat64Operations(),
    ArrayOfNullableFloat64Operations(),
    None,
]
