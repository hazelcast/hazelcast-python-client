from hazelcast.exception import HazelcastSerializationError
from hazelcast.serialization import bits
from hazelcast.serialization.api import PortableReader
from hazelcast.serialization.portable.classdef import FieldType
from hazelcast.six.moves import range

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
            raise ValueError("Field count({}) in stream does not match! {}".format(field_count, class_def))
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
        return self._in.read_boolean(pos)

    def read_byte(self, field_name):
        pos = self._read_position(field_name, FieldType.BYTE)
        return self._in.read_byte(pos)

    def read_char(self, field_name):
        pos = self._read_position(field_name, FieldType.CHAR)
        return self._in.read_char(pos)

    def read_short(self, field_name):
        pos = self._read_position(field_name, FieldType.SHORT)
        return self._in.read_short(pos)

    def read_int(self, field_name):
        pos = self._read_position(field_name, FieldType.INT)
        return self._in.read_int(pos)

    def read_long(self, field_name):
        pos = self._read_position(field_name, FieldType.LONG)
        return self._in.read_long(pos)

    def read_float(self, field_name):
        pos = self._read_position(field_name, FieldType.FLOAT)
        return self._in.read_float(pos)

    def read_double(self, field_name):
        pos = self._read_position(field_name, FieldType.DOUBLE)
        return self._in.read_double(pos)

    def read_utf(self, field_name):
        cur_pos = self._in.position()
        try:
            pos = self._read_position(field_name, FieldType.UTF)
            self._in.set_position(pos)
            return self._in.read_utf()
        finally:
            self._in.set_position(cur_pos)

    def read_portable(self, field_name):
        cur_pos = self._in.position()
        try:
            fd = self._class_def.get_field(field_name)
            if fd is None:
                raise self._create_unknown_field_exception(field_name)
            if fd.field_type != FieldType.PORTABLE:
                raise HazelcastSerializationError("Not a Portable field: {}".format(field_name))

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

    def read_utf_array(self, field_name):
        current_pos = self._in.position()
        try:
            pos = self._read_position(field_name, FieldType.UTF_ARRAY)
            self._in.set_position(pos)
            return self._in.read_utf_array()
        finally:
            self._in.set_position(current_pos)

    def read_portable_array(self, field_name):
        current_pos = self._in.position()
        try:
            fd = self._class_def.get_field(field_name)
            if fd is None:
                raise self._create_unknown_field_exception(field_name)
            if fd.field_type != FieldType.PORTABLE_ARRAY:
                raise HazelcastSerializationError("Not a portable array field: {}".format(field_name))

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
                    start = self._in.read_int(offset + i * bits.INT_SIZE_IN_BYTES)
                    self._in.set_position(start)
                    portables[i] = self._portable_serializer.read_internal(self._in, factory_id, class_id)
            return portables
        finally:
            self._in.set_position(current_pos)

    def get_raw_data_input(self):
        if not self._raw:
            pos = self._in.read_int(self._offset + self._class_def.get_field_count() * bits.INT_SIZE_IN_BYTES)
            self._in.set_position(pos)
        self._raw = True
        return self._in

    def end(self):
        self._in.set_position(self._final_pos)

    def _read_position(self, field_name, field_type):
        if self._raw:
            raise HazelcastSerializationError("Cannot read Portable fields after get_raw_data_input() is called!")
        fd = self._class_def.get_field(field_name)
        if fd is None:
            return self._read_nested_position(field_name, field_type)
        if fd.field_type != field_type:
            raise HazelcastSerializationError("Not a '{}' field: {}".format(field_type, field_name))
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
                    raise ValueError("Parent field is null: ".format(field_names[i]))
                _reader = self._portable_serializer.create_default_reader(self._in)
            if fd is None:
                raise self._create_unknown_field_exception(field_name)
            if fd.field_type != field_type:
                raise HazelcastSerializationError("Not a '{}' field: {}".format(field_type, field_name))
            return _reader._read_position_by_field_def(fd)
        raise self._create_unknown_field_exception(field_name)

    def _create_unknown_field_exception(self, field_name):
        return HazelcastSerializationError("Unknown field name: '{}' for ClassDefinition[ id: {}, version: {} ]"
                                           .format(field_name, self._class_def.class_id, self._class_def.version))

    def _read_position_by_field_def(self, fd):
        pos = self._in.read_int(self._offset + fd.index * bits.INT_SIZE_IN_BYTES)
        _len = self._in.read_short(pos)
        # name + len + type
        return pos + bits.SHORT_SIZE_IN_BYTES + _len + 1


def _check_factory_and_class(field_def, factory_id, class_id):
    if factory_id != field_def.factory_id:
        raise ValueError("Invalid factoryId! Expected: {}, Current: {}".format(factory_id, field_def.factory_id))
    if class_id != field_def.class_id:
        raise ValueError("Invalid classId! Expected: {}, Current: {}".format(class_id, field_def.class_id))


class MorphingPortableReader(DefaultPortableReader):

    def read_short(self, field_name):
        fd = self._class_def.get_field(field_name)
        if fd is None:
            return 0
        elif fd.field_type == FieldType.SHORT:
            return super(MorphingPortableReader, self).read_short(field_name)
        elif fd.field_type == FieldType.BYTE:
            return super(MorphingPortableReader, self).read_byte(field_name)
        else:
            raise self.create_incompatible_class_change_error(fd, FieldType.SHORT)

    def read_int(self, field_name):
        fd = self._class_def.get_field(field_name)
        if fd is None:
            return 0
        elif fd.field_type == FieldType.INT:
            return super(MorphingPortableReader, self).read_int(field_name)
        elif fd.field_type == FieldType.BYTE:
            return super(MorphingPortableReader, self).read_byte(field_name)
        elif fd.field_type == FieldType.CHAR:
            return super(MorphingPortableReader, self).read_char(field_name)
        elif fd.field_type == FieldType.SHORT:
            return super(MorphingPortableReader, self).read_short(field_name)
        else:
            raise self.create_incompatible_class_change_error(fd, FieldType.INT)

    def read_long(self, field_name):
        fd = self._class_def.get_field(field_name)
        if fd is None:
            return 0
        elif fd.field_type == FieldType.LONG:
            return super(MorphingPortableReader, self).read_long(field_name)
        elif fd.field_type == FieldType.INT:
            return super(MorphingPortableReader, self).read_int(field_name)
        elif fd.field_type == FieldType.BYTE:
            return super(MorphingPortableReader, self).read_byte(field_name)
        elif fd.field_type == FieldType.CHAR:
            return super(MorphingPortableReader, self).read_char(field_name)
        elif fd.field_type == FieldType.SHORT:
            return super(MorphingPortableReader, self).read_short(field_name)
        else:
            raise self.create_incompatible_class_change_error(fd, FieldType.LONG)

    def read_float(self, field_name):
        fd = self._class_def.get_field(field_name)
        if fd is None:
            return 0
        elif fd.field_type == FieldType.FLOAT:
            return super(MorphingPortableReader, self).read_float(field_name)
        elif fd.field_type == FieldType.INT:
            return super(MorphingPortableReader, self).read_int(field_name)
        elif fd.field_type == FieldType.BYTE:
            return super(MorphingPortableReader, self).read_byte(field_name)
        elif fd.field_type == FieldType.CHAR:
            return super(MorphingPortableReader, self).read_char(field_name)
        elif fd.field_type == FieldType.SHORT:
            return super(MorphingPortableReader, self).read_short(field_name)
        else:
            raise self.create_incompatible_class_change_error(fd, FieldType.FLOAT)

    def read_double(self, field_name):
        fd = self._class_def.get_field(field_name)
        if fd is None:
            return 0.0
        elif fd.field_type == FieldType.DOUBLE:
            return super(MorphingPortableReader, self).read_double(field_name)
        elif fd.field_type == FieldType.LONG:
            return super(MorphingPortableReader, self).read_long(field_name)
        elif fd.field_type == FieldType.FLOAT:
            return super(MorphingPortableReader, self).read_float(field_name)
        elif fd.field_type == FieldType.INT:
            return super(MorphingPortableReader, self).read_int(field_name)
        elif fd.field_type == FieldType.BYTE:
            return super(MorphingPortableReader, self).read_byte(field_name)
        elif fd.field_type == FieldType.CHAR:
            return super(MorphingPortableReader, self).read_char(field_name)
        elif fd.field_type == FieldType.SHORT:
            return super(MorphingPortableReader, self).read_short(field_name)
        else:
            raise self.create_incompatible_class_change_error(fd, FieldType.DOUBLE)

    def read_byte(self, field_name):
        fd = self._class_def.get_field(field_name)
        if fd is None:
            return 0
        self.validate_type_compatibility(fd, FieldType.BYTE)
        return super(MorphingPortableReader, self).read_byte(field_name)

    def read_boolean(self, field_name):
        fd = self._class_def.get_field(field_name)
        if fd is None:
            return False
        self.validate_type_compatibility(fd, FieldType.BOOLEAN)
        return super(MorphingPortableReader, self).read_boolean(field_name)

    def read_char(self, field_name):
        fd = self._class_def.get_field(field_name)
        if fd is None:
            return 0
        self.validate_type_compatibility(fd, FieldType.CHAR)
        return super(MorphingPortableReader, self).read_char(field_name)

    def read_utf(self, field_name):
        fd = self._class_def.get_field(field_name)
        if fd is None:
            return None
        self.validate_type_compatibility(fd, FieldType.UTF)
        return super(MorphingPortableReader, self).read_utf(field_name)

    def read_utf_array(self, field_name):
        fd = self._class_def.get_field(field_name)
        if fd is None:
            return None
        self.validate_type_compatibility(fd, FieldType.UTF_ARRAY)
        return super(MorphingPortableReader, self).read_utf_array(field_name)

    def read_short_array(self, field_name):
        fd = self._class_def.get_field(field_name)
        if fd is None:
            return None
        self.validate_type_compatibility(fd, FieldType.SHORT_ARRAY)
        return super(MorphingPortableReader, self).read_short_array(field_name)

    def read_int_array(self, field_name):
        fd = self._class_def.get_field(field_name)
        if fd is None:
            return None
        self.validate_type_compatibility(fd, FieldType.INT_ARRAY)
        return super(MorphingPortableReader, self).read_int_array(field_name)

    def read_long_array(self, field_name):
        fd = self._class_def.get_field(field_name)
        if fd is None:
            return None
        self.validate_type_compatibility(fd, FieldType.LONG_ARRAY)
        return super(MorphingPortableReader, self).read_long_array(field_name)

    def read_float_array(self, field_name):
        fd = self._class_def.get_field(field_name)
        if fd is None:
            return None
        self.validate_type_compatibility(fd, FieldType.FLOAT_ARRAY)
        return super(MorphingPortableReader, self).read_float_array(field_name)

    def read_double_array(self, field_name):
        fd = self._class_def.get_field(field_name)
        if fd is None:
            return None
        self.validate_type_compatibility(fd, FieldType.DOUBLE_ARRAY)
        return super(MorphingPortableReader, self).read_double_array(field_name)

    def read_char_array(self, field_name):
        fd = self._class_def.get_field(field_name)
        if fd is None:
            return None
        self.validate_type_compatibility(fd, FieldType.CHAR_ARRAY)
        return super(MorphingPortableReader, self).read_char_array(field_name)

    def read_byte_array(self, field_name):
        fd = self._class_def.get_field(field_name)
        if fd is None:
            return None
        self.validate_type_compatibility(fd, FieldType.BYTE_ARRAY)
        return super(MorphingPortableReader, self).read_byte_array(field_name)

    def read_boolean_array(self, field_name):
        fd = self._class_def.get_field(field_name)
        if fd is None:
            return None
        self.validate_type_compatibility(fd, FieldType.BOOLEAN_ARRAY)
        return super(MorphingPortableReader, self).read_boolean_array(field_name)

    def read_portable(self, field_name):
        fd = self._class_def.get_field(field_name)
        if fd is None:
            return None
        self.validate_type_compatibility(fd, FieldType.PORTABLE)
        return super(MorphingPortableReader, self).read_portable(field_name)

    def read_portable_array(self, field_name):
        fd = self._class_def.get_field(field_name)
        if fd is None:
            return None
        self.validate_type_compatibility(fd, FieldType.PORTABLE_ARRAY)
        return super(MorphingPortableReader, self).read_portable_array(field_name)

    def validate_type_compatibility(self, field_def, expected_type):
        if field_def.field_type != expected_type:
            raise self.create_incompatible_class_change_error(field_def, expected_type)

    def create_incompatible_class_change_error(self, field_def, expected_type):
        return TypeError("Incompatible to read {} from {} while reading field :{} on {}"
                         .format(expected_type, field_def.field_type, field_def.field_name, self._class_def))
