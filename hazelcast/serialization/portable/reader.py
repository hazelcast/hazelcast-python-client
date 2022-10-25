import datetime
import time

from hazelcast.errors import HazelcastSerializationError
from hazelcast.serialization import bits, NULL_ARRAY_LENGTH
from hazelcast.serialization.api import PortableReader, ObjectDataInput
from hazelcast.serialization.portable.classdef import FieldType
from hazelcast.serialization.util import IOUtil


class DefaultPortableReader(PortableReader):
    def __init__(self, portable_serializer, data_input, class_def):
        self._portable_serializer = portable_serializer
        self._in = data_input
        self._class_def = class_def
        try:
            # final position after portable is read
            self._final_pos = data_input.read_int()
            # field count
            field_count = data_input.read_int()
        except Exception:
            raise HazelcastSerializationError()
        if field_count != class_def.get_field_count():
            raise ValueError(
                "Field count(%s) in stream does not match! %s" % (field_count, class_def)
            )
        self._offset = data_input.position()
        self._raw = False

    def get_version(self):
        return self._class_def.version

    def has_field(self, field_name):
        return self._class_def.has_field(field_name)

    def get_field_names(self):
        return self._class_def.get_field_names()

    def get_field_type(self, field_name):
        return self._class_def.get_field_type(field_name)

    def get_field_class_id(self, field_name):
        return self._class_def.get_field_class_id(field_name)

    def read_boolean(self, field_name):
        pos = self._read_position(field_name, FieldType.BOOLEAN)
        return self._in.read_boolean_positional(pos)

    def read_byte(self, field_name):
        pos = self._read_position(field_name, FieldType.BYTE)
        return self._in.read_byte_positional(pos)

    def read_char(self, field_name):
        pos = self._read_position(field_name, FieldType.CHAR)
        return self._in.read_char_positional(pos)

    def read_short(self, field_name):
        pos = self._read_position(field_name, FieldType.SHORT)
        return self._in.read_short_positional(pos)

    def read_int(self, field_name):
        pos = self._read_position(field_name, FieldType.INT)
        return self._in.read_int_positional(pos)

    def read_long(self, field_name):
        pos = self._read_position(field_name, FieldType.LONG)
        return self._in.read_long_positional(pos)

    def read_float(self, field_name):
        pos = self._read_position(field_name, FieldType.FLOAT)
        return self._in.read_float_positional(pos)

    def read_double(self, field_name):
        pos = self._read_position(field_name, FieldType.DOUBLE)
        return self._in.read_double_positional(pos)

    def read_string(self, field_name):
        cur_pos = self._in.position()
        try:
            pos = self._read_position(field_name, FieldType.STRING)
            self._in.set_position(pos)
            return self._in.read_string()
        finally:
            self._in.set_position(cur_pos)

    def read_portable(self, field_name):
        cur_pos = self._in.position()
        try:
            fd = self._class_def.get_field(field_name)
            if fd is None:
                raise self._create_unknown_field_exception(field_name)
            if fd.field_type != FieldType.PORTABLE:
                raise HazelcastSerializationError("Not a Portable field: %s" % field_name)

            pos = self._read_position_by_field_def(fd)
            self._in.set_position(pos)

            is_none = self._in.read_boolean()
            factory_id = self._in.read_int()
            class_id = self._in.read_int()

            _check_factory_and_class(fd, factory_id, class_id)

            if is_none:
                return None
            return self._portable_serializer.read_internal(self._in, factory_id, class_id)
        finally:
            self._in.set_position(cur_pos)

    def read_decimal(self, field_name):
        return self._read_nullable_field(field_name, FieldType.DECIMAL, IOUtil.read_big_decimal)

    def read_time(self, field_name):
        return self._read_nullable_field(field_name, FieldType.TIME, _read_portable_time)

    def read_date(self, field_name):
        return self._read_nullable_field(field_name, FieldType.DATE, _read_portable_date)

    def read_timestamp(self, field_name):
        return self._read_nullable_field(field_name, FieldType.TIMESTAMP, _read_portable_timestamp)

    def read_timestamp_with_timezone(self, field_name):
        return self._read_nullable_field(
            field_name, FieldType.TIMESTAMP_WITH_TIMEZONE, _read_portable_timestamp_with_timezone
        )

    def read_boolean_array(self, field_name):
        current_pos = self._in.position()
        try:
            pos = self._read_position(field_name, FieldType.BOOLEAN_ARRAY)
            self._in.set_position(pos)
            return self._in.read_boolean_array()
        finally:
            self._in.set_position(current_pos)

    def read_byte_array(self, field_name):
        current_pos = self._in.position()
        try:
            pos = self._read_position(field_name, FieldType.BYTE_ARRAY)
            self._in.set_position(pos)
            return self._in.read_byte_array()
        finally:
            self._in.set_position(current_pos)

    def read_char_array(self, field_name):
        current_pos = self._in.position()
        try:
            pos = self._read_position(field_name, FieldType.CHAR_ARRAY)
            self._in.set_position(pos)
            return self._in.read_char_array()
        finally:
            self._in.set_position(current_pos)

    def read_short_array(self, field_name):
        pass
        current_pos = self._in.position()
        try:
            pos = self._read_position(field_name, FieldType.SHORT_ARRAY)
            self._in.set_position(pos)
            return self._in.read_short_array()
        finally:
            self._in.set_position(current_pos)

    def read_int_array(self, field_name):
        current_pos = self._in.position()
        try:
            pos = self._read_position(field_name, FieldType.INT_ARRAY)
            self._in.set_position(pos)
            return self._in.read_int_array()
        finally:
            self._in.set_position(current_pos)

    def read_long_array(self, field_name):
        current_pos = self._in.position()
        try:
            pos = self._read_position(field_name, FieldType.LONG_ARRAY)
            self._in.set_position(pos)
            return self._in.read_long_array()
        finally:
            self._in.set_position(current_pos)

    def read_float_array(self, field_name):
        current_pos = self._in.position()
        try:
            pos = self._read_position(field_name, FieldType.FLOAT_ARRAY)
            self._in.set_position(pos)
            return self._in.read_float_array()
        finally:
            self._in.set_position(current_pos)

    def read_double_array(self, field_name):
        current_pos = self._in.position()
        try:
            pos = self._read_position(field_name, FieldType.DOUBLE_ARRAY)
            self._in.set_position(pos)
            return self._in.read_double_array()
        finally:
            self._in.set_position(current_pos)

    def read_string_array(self, field_name):
        current_pos = self._in.position()
        try:
            pos = self._read_position(field_name, FieldType.STRING_ARRAY)
            self._in.set_position(pos)
            return self._in.read_string_array()
        finally:
            self._in.set_position(current_pos)

    def read_portable_array(self, field_name):
        current_pos = self._in.position()
        try:
            fd = self._class_def.get_field(field_name)
            if fd is None:
                raise self._create_unknown_field_exception(field_name)
            if fd.field_type != FieldType.PORTABLE_ARRAY:
                raise HazelcastSerializationError("Not a portable array field: %s" % field_name)

            pos = self._read_position_by_field_def(fd)
            self._in.set_position(pos)

            length = self._in.read_int()
            factory_id = self._in.read_int()
            class_id = self._in.read_int()
            if length == bits.NULL_ARRAY_LENGTH:
                return None

            _check_factory_and_class(fd, factory_id, class_id)

            portables = [None] * length
            if length > 0:
                offset = self._in.position()
                for i in range(0, length):
                    start = self._in.read_int_positional(offset + i * bits.INT_SIZE_IN_BYTES)
                    self._in.set_position(start)
                    portables[i] = self._portable_serializer.read_internal(
                        self._in, factory_id, class_id
                    )
            return portables
        finally:
            self._in.set_position(current_pos)

    def read_decimal_array(self, field_name):
        return self._read_object_array_field(
            field_name, FieldType.DECIMAL_ARRAY, IOUtil.read_big_decimal
        )

    def read_time_array(self, field_name):
        return self._read_object_array_field(field_name, FieldType.TIME_ARRAY, _read_portable_time)

    def read_date_array(self, field_name):
        return self._read_object_array_field(field_name, FieldType.DATE_ARRAY, _read_portable_date)

    def read_timestamp_array(self, field_name):
        return self._read_object_array_field(
            field_name, FieldType.TIMESTAMP_ARRAY, _read_portable_timestamp
        )

    def read_timestamp_with_timezone_array(self, field_name):
        return self._read_object_array_field(
            field_name,
            FieldType.TIMESTAMP_WITH_TIMEZONE_ARRAY,
            _read_portable_timestamp_with_timezone,
        )

    def read_utf(self, field_name):
        return self.read_string(field_name)

    def read_utf_array(self, field_name):
        return self.read_string_array(field_name)

    def _read_nullable_field(self, field_name, field_type, func):
        current_pos = self._in.position()
        try:
            pos = self._read_position(field_name, field_type)
            self._in.set_position(pos)
            is_none = self._in.read_boolean()
            if is_none:
                return None
            return func(self._in)
        finally:
            self._in.set_position(current_pos)

    def _read_object_array_field(self, field_name, field_type: FieldType, func):
        current_pos = self._in.position()
        try:
            pos = self._read_position(field_name, field_type)
            self._in.set_position(pos)
            length = self._in.read_int()
            if length == NULL_ARRAY_LENGTH:
                return None
            values = [None] * length
            if length > 0:
                offset = self._in.position()
                for i in range(length):
                    pos = self._in.read_int_positional(offset + i * bits.INT_SIZE_IN_BYTES)
                    self._in.set_position(pos)
                    values[i] = func(self._in)
            return values
        finally:
            self._in.set_position(current_pos)

    def get_raw_data_input(self):
        if not self._raw:
            pos = self._in.read_int_positional(
                self._offset + self._class_def.get_field_count() * bits.INT_SIZE_IN_BYTES
            )
            self._in.set_position(pos)
        self._raw = True
        return self._in

    def end(self):
        self._in.set_position(self._final_pos)

    def _read_position(self, field_name, field_type):
        if self._raw:
            raise HazelcastSerializationError(
                "Cannot read Portable fields after get_raw_data_input() is called!"
            )
        fd = self._class_def.get_field(field_name)
        if fd is None:
            return self._read_nested_position(field_name, field_type)
        if fd.field_type != field_type:
            raise HazelcastSerializationError("Not a '%s' field: %s" % (field_type, field_name))
        return self._read_position_by_field_def(fd)

    def _read_nested_position(self, field_name, field_type):
        field_names = field_name.split(".")
        if len(field_names) > 1:
            fd = None
            _reader = self
            for i in range(0, len(field_names)):
                fd = _reader._class_def.get_field(field_names[i])
                if fd is None:
                    break
                if i == len(field_names) - 1:
                    break
                pos = _reader._read_position_by_field_def(fd)
                self._in.set_position(pos)
                is_none = self._in.read_boolean()
                if is_none:
                    raise ValueError("Parent field is null: %s" % field_names[i])
                _reader = self._portable_serializer.create_default_reader(self._in)
            if fd is None:
                raise self._create_unknown_field_exception(field_name)
            if fd.field_type != field_type:
                raise HazelcastSerializationError("Not a '%s' field: %s" % (field_type, field_name))
            return _reader._read_position_by_field_def(fd)
        raise self._create_unknown_field_exception(field_name)

    def _create_unknown_field_exception(self, field_name):
        return HazelcastSerializationError(
            "Unknown field name: '%s' for ClassDefinition(id=%s, version=%s)"
            % (field_name, self._class_def.class_id, self._class_def.version)
        )

    def _read_position_by_field_def(self, fd):
        pos = self._in.read_int_positional(self._offset + fd.index * bits.INT_SIZE_IN_BYTES)
        _len = self._in.read_short_positional(pos)
        # name + len + type
        return pos + bits.SHORT_SIZE_IN_BYTES + _len + 1


def _check_factory_and_class(field_def, factory_id, class_id):
    if factory_id != field_def.factory_id:
        raise ValueError(
            "Invalid factoryId! Expected: %s, Current: %s" % (factory_id, field_def.factory_id)
        )
    if class_id != field_def.class_id:
        raise ValueError(
            "Invalid classId! Expected: %s, Current: %s" % (class_id, field_def.class_id)
        )


def _read_portable_date(inp: ObjectDataInput):
    year, month, day = _read_portable_date_helper(inp)
    return datetime.date(year, month, day)


def _read_portable_date_helper(inp: ObjectDataInput):
    year = inp.read_short()
    month = inp.read_byte()
    day = inp.read_byte()
    return year, month, day


def _read_portable_time(inp: ObjectDataInput):
    hour, minute, second, microsecond = _read_portable_time_helper(inp)
    return datetime.time(hour, minute, second, microsecond)


def _read_portable_time_helper(inp: ObjectDataInput):
    hour = inp.read_byte()
    minute = inp.read_byte()
    second = inp.read_byte()
    microsecond = inp.read_int() // 1000
    return hour, minute, second, microsecond


def _read_portable_timestamp(inp: ObjectDataInput):
    year, month, day = _read_portable_date_helper(inp)
    hour, minute, second, microsecond = _read_portable_time_helper(inp)
    return datetime.datetime(year, month, day, hour, minute, second, microsecond)


def _read_portable_timestamp_with_timezone(inp: ObjectDataInput):
    year, month, day = _read_portable_date_helper(inp)
    hour, minute, second, microsecond = _read_portable_time_helper(inp)
    offset = inp.read_int()
    return datetime.datetime(
        year,
        month,
        day,
        hour,
        minute,
        second,
        microsecond,
        datetime.timezone(datetime.timedelta(seconds=offset)),
    )


class MorphingPortableReader(DefaultPortableReader):
    def read_short(self, field_name):
        fd = self._class_def.get_field(field_name)
        if fd is None:
            return 0
        elif fd.field_type == FieldType.SHORT:
            return super().read_short(field_name)
        elif fd.field_type == FieldType.BYTE:
            return super().read_byte(field_name)
        else:
            raise self.create_incompatible_class_change_error(fd, FieldType.SHORT)

    def read_int(self, field_name):
        fd = self._class_def.get_field(field_name)
        if fd is None:
            return 0
        elif fd.field_type == FieldType.INT:
            return super().read_int(field_name)
        elif fd.field_type == FieldType.BYTE:
            return super().read_byte(field_name)
        elif fd.field_type == FieldType.CHAR:
            return super().read_char(field_name)
        elif fd.field_type == FieldType.SHORT:
            return super().read_short(field_name)
        else:
            raise self.create_incompatible_class_change_error(fd, FieldType.INT)

    def read_long(self, field_name):
        fd = self._class_def.get_field(field_name)
        if fd is None:
            return 0
        elif fd.field_type == FieldType.LONG:
            return super().read_long(field_name)
        elif fd.field_type == FieldType.INT:
            return super().read_int(field_name)
        elif fd.field_type == FieldType.BYTE:
            return super().read_byte(field_name)
        elif fd.field_type == FieldType.CHAR:
            return super().read_char(field_name)
        elif fd.field_type == FieldType.SHORT:
            return super().read_short(field_name)
        else:
            raise self.create_incompatible_class_change_error(fd, FieldType.LONG)

    def read_float(self, field_name):
        fd = self._class_def.get_field(field_name)
        if fd is None:
            return 0
        elif fd.field_type == FieldType.FLOAT:
            return super().read_float(field_name)
        elif fd.field_type == FieldType.INT:
            return super().read_int(field_name)
        elif fd.field_type == FieldType.BYTE:
            return super().read_byte(field_name)
        elif fd.field_type == FieldType.CHAR:
            return super().read_char(field_name)
        elif fd.field_type == FieldType.SHORT:
            return super().read_short(field_name)
        else:
            raise self.create_incompatible_class_change_error(fd, FieldType.FLOAT)

    def read_double(self, field_name):
        fd = self._class_def.get_field(field_name)
        if fd is None:
            return 0.0
        elif fd.field_type == FieldType.DOUBLE:
            return super().read_double(field_name)
        elif fd.field_type == FieldType.LONG:
            return super().read_long(field_name)
        elif fd.field_type == FieldType.FLOAT:
            return super().read_float(field_name)
        elif fd.field_type == FieldType.INT:
            return super().read_int(field_name)
        elif fd.field_type == FieldType.BYTE:
            return super().read_byte(field_name)
        elif fd.field_type == FieldType.CHAR:
            return super().read_char(field_name)
        elif fd.field_type == FieldType.SHORT:
            return super().read_short(field_name)
        else:
            raise self.create_incompatible_class_change_error(fd, FieldType.DOUBLE)

    def read_byte(self, field_name):
        fd = self._class_def.get_field(field_name)
        if fd is None:
            return 0
        self.validate_type_compatibility(fd, FieldType.BYTE)
        return super().read_byte(field_name)

    def read_boolean(self, field_name):
        fd = self._class_def.get_field(field_name)
        if fd is None:
            return False
        self.validate_type_compatibility(fd, FieldType.BOOLEAN)
        return super().read_boolean(field_name)

    def read_char(self, field_name):
        fd = self._class_def.get_field(field_name)
        if fd is None:
            return 0
        self.validate_type_compatibility(fd, FieldType.CHAR)
        return super().read_char(field_name)

    def read_string(self, field_name):
        return self._validate_type_compatibility_and_read(
            field_name, FieldType.STRING, super().read_string
        )

    def read_decimal(self, field_name):
        return self._validate_type_compatibility_and_read(
            field_name, FieldType.DECIMAL, super().read_decimal
        )

    def read_time(self, field_name):
        return self._validate_type_compatibility_and_read(
            field_name, FieldType.TIME, super().read_time
        )

    def read_date(self, field_name):
        return self._validate_type_compatibility_and_read(
            field_name, FieldType.DATE, super().read_date
        )

    def read_timestamp(self, field_name):
        return self._validate_type_compatibility_and_read(
            field_name, FieldType.TIMESTAMP, super().read_timestamp
        )

    def read_timestamp_with_timezone(self, field_name):
        return self._validate_type_compatibility_and_read(
            field_name,
            FieldType.TIMESTAMP_WITH_TIMEZONE,
            super().read_timestamp_with_timezone,
        )

    def read_string_array(self, field_name):
        return self._validate_type_compatibility_and_read(
            field_name,
            FieldType.STRING_ARRAY,
            super().read_string_array,
        )

    def read_short_array(self, field_name):
        return self._validate_type_compatibility_and_read(
            field_name, FieldType.SHORT_ARRAY, super().read_short_array
        )

    def read_int_array(self, field_name):
        return self._validate_type_compatibility_and_read(
            field_name, FieldType.INT_ARRAY, super().read_int_array
        )

    def read_long_array(self, field_name):
        return self._validate_type_compatibility_and_read(
            field_name, FieldType.LONG_ARRAY, super().read_long_array
        )

    def read_float_array(self, field_name):
        return self._validate_type_compatibility_and_read(
            field_name, FieldType.FLOAT_ARRAY, super().read_float_array
        )

    def read_double_array(self, field_name):
        return self._validate_type_compatibility_and_read(
            field_name,
            FieldType.DOUBLE_ARRAY,
            super().read_double_array,
        )

    def read_char_array(self, field_name):
        return self._validate_type_compatibility_and_read(
            field_name, FieldType.CHAR_ARRAY, super().read_char_array
        )

    def read_byte_array(self, field_name):
        return self._validate_type_compatibility_and_read(
            field_name, FieldType.BYTE_ARRAY, super().read_byte_array
        )

    def read_boolean_array(self, field_name):
        return self._validate_type_compatibility_and_read(
            field_name,
            FieldType.BOOLEAN_ARRAY,
            super().read_boolean_array,
        )

    def read_portable(self, field_name):
        return self._validate_type_compatibility_and_read(
            field_name, FieldType.PORTABLE, super().read_portable
        )

    def read_portable_array(self, field_name):
        return self._validate_type_compatibility_and_read(
            field_name,
            FieldType.PORTABLE_ARRAY,
            super().read_portable_array,
        )

    def read_utf(self, field_name):
        return self.read_string(field_name)

    def read_utf_array(self, field_name):
        return self.read_string_array(field_name)

    def read_decimal_array(self, field_name):
        return self._validate_type_compatibility_and_read(
            field_name, FieldType.DECIMAL_ARRAY, super().read_decimal_array
        )

    def read_time_array(self, field_name):
        return self._validate_type_compatibility_and_read(
            field_name, FieldType.TIME_ARRAY, super().read_time_array
        )

    def read_date_array(self, field_name):
        return self._validate_type_compatibility_and_read(
            field_name, FieldType.DATE_ARRAY, super().read_date_array
        )

    def read_timestamp_array(self, field_name):
        return self._validate_type_compatibility_and_read(
            field_name, FieldType.TIMESTAMP_ARRAY, super().read_timestamp_array
        )

    def read_timestamp_with_timezone_array(self, field_name):
        return self._validate_type_compatibility_and_read(
            field_name,
            FieldType.TIMESTAMP_WITH_TIMEZONE_ARRAY,
            super().read_timestamp_with_timezone_array,
        )

    def validate_type_compatibility(self, field_def, expected_type):
        if field_def.field_type != expected_type:
            raise self.create_incompatible_class_change_error(field_def, expected_type)

    def create_incompatible_class_change_error(self, field_def, expected_type):
        return TypeError(
            "Incompatible to read %s from %s while reading field: %s on %s"
            % (expected_type, field_def.field_type, field_def.field_name, self._class_def)
        )

    def _validate_type_compatibility_and_read(self, field_name, field_type, reader):
        fd = self._class_def.get_field(field_name)
        if fd is None:
            return None
        self.validate_type_compatibility(fd, field_type)
        return reader(field_name)
