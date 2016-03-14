import hazelcast.util as util
from hazelcast.exception import HazelcastSerializationError
from hazelcast.serialization import INT_SIZE_IN_BYTES, NULL_ARRAY_LENGTH
from hazelcast.serialization.api import PortableWriter
from hazelcast.serialization.output import EmptyObjectDataOutput
from hazelcast.serialization.portable.classdef import FieldType, ClassDefinitionBuilder


class DefaultPortableWriter(PortableWriter):
    def __init__(self, portable_serializer, out, class_definition):
        self._raw = False
        self._portable_serializer = portable_serializer
        self._out = out
        self._class_def = class_definition
        self._writen_fields = set()
        self._begin_pos = out.position()

        # room for final offset
        out.write_zero_bytes(4)
        out.write_int(class_definition.get_field_count())

        self._offset = out.position()
        # one additional for raw data
        field_indexes_length = (class_definition.get_field_count() + 1) * INT_SIZE_IN_BYTES
        out.write_zero_bytes(field_indexes_length)

    def write_byte(self, field_name, value):
        self._set_position(field_name, FieldType.BYTE)
        self._out.write_byte(value)

    def write_boolean(self, field_name, value):
        self._set_position(field_name, FieldType.BOOLEAN)
        self._out.write_boolean(value)

    def write_char(self, field_name, value):
        self._set_position(field_name, FieldType.CHAR)
        self._out.write_char(value)

    def write_short(self, field_name, value):
        self._set_position(field_name, FieldType.SHORT)
        self._out.write_short(value)

    def write_int(self, field_name, value):
        self._set_position(field_name, FieldType.INT)
        self._out.write_int(value)

    def write_long(self, field_name, value):
        self._set_position(field_name, FieldType.LONG)
        self._out.write_long(value)

    def write_float(self, field_name, value):
        self._set_position(field_name, FieldType.FLOAT)
        self._out.write_float(value)

    def write_double(self, field_name, value):
        self._set_position(field_name, FieldType.DOUBLE)
        self._out.write_double(value)

    def write_utf(self, field_name, value):
        self._set_position(field_name, FieldType.UTF)
        self._out.write_utf(value)

    def write_byte_array(self, field_name, values):
        self._set_position(field_name, FieldType.BYTE_ARRAY)
        self._out.write_byte_array(values)

    def write_boolean_array(self, field_name, values):
        self._set_position(field_name, FieldType.BOOLEAN_ARRAY)
        self._out.write_boolean_array(values)

    def write_char_array(self, field_name, values):
        self._set_position(field_name, FieldType.CHAR_ARRAY)
        self._out.write_char_array(values)

    def write_short_array(self, field_name, values):
        self._set_position(field_name, FieldType.SHORT_ARRAY)
        self._out.write_short_array(values)

    def write_int_array(self, field_name, values):
        self._set_position(field_name, FieldType.INT_ARRAY)
        self._out.write_int_array(values)

    def write_long_array(self, field_name, values):
        self._set_position(field_name, FieldType.LONG_ARRAY)
        self._out.write_long_array(values)

    def write_float_array(self, field_name, values):
        self._set_position(field_name, FieldType.FLOAT_ARRAY)
        self._out.write_float_array(values)

    def write_double_array(self, field_name, values):
        self._set_position(field_name, FieldType.DOUBLE_ARRAY)
        self._out.write_double_array(values)

    def write_utf_array(self, field_name, values):
        self._set_position(field_name, FieldType.UTF_ARRAY)
        self._out.write_utf_array(values)

    def write_portable(self, field_name, portable):
        fd = self._set_position(field_name, FieldType.PORTABLE)
        is_none = portable is None
        self._out.write_boolean(is_none)

        self._out.write_int(fd.factory_id)
        self._out.write_int(fd.class_id)

        if not is_none:
            _check_portable_attributes(fd, portable)
            self._portable_serializer.write_internal(self._out, portable)

    def write_portable_array(self, field_name, values):
        fd = self._set_position(field_name, FieldType.PORTABLE_ARRAY)
        _len = NULL_ARRAY_LENGTH if values is None else len(values)
        self._out.write_int(_len)
        self._out.write_int(fd.factory_id)
        self._out.write_int(fd.class_id)
        if _len > 0:
            _offset = self._out.position()
            self._out.write_zero_bytes(_len * 4)
            for i in xrange(0, _len):
                portable = values[i]
                _check_portable_attributes(fd, portable)
                _pos_val = self._out.position()
                self._out.write_int(_pos_val, _offset + i * INT_SIZE_IN_BYTES)
                self._portable_serializer.write_internal(self._out, portable)

    def write_null_portable(self, field_name, factory_id, class_id):
        self._set_position(field_name, FieldType.PORTABLE)
        self._out.write_boolean(True)
        self._out.write_int(factory_id)
        self._out.write_int(class_id)

    def get_raw_data_output(self):
        if not self._raw:
            _pos_val = self._out.position()
            # last index
            _index = self._class_def.get_field_count()
            self._out.write_int(_pos_val, self._offset + _index * INT_SIZE_IN_BYTES)
        self._raw = True
        return self._out

    # internal
    def _set_position(self, field_name, field_type):
        if self._raw:
            raise HazelcastSerializationError("Cannot write Portable fields after get_raw_data_output() is called!")
        fd = self._class_def.get_field(field_name)
        if fd is None:
            raise HazelcastSerializationError("Invalid field name:'{}' for ClassDefinition(id:{} , version:{} )"
                                              .format(field_name, self._class_def.class_id, self._class_def.version))
        if field_name not in self._writen_fields:
            self._write_field_def(fd.index, field_name, field_type)
            self._writen_fields.add(field_name)
        else:
            raise HazelcastSerializationError("Field '{}' has already been written!".format(field_name))
        return fd

    def _write_field_def(self, index, field_name, field_type):
        pos_val = self._out.position()
        position = self._offset + index * INT_SIZE_IN_BYTES
        self._out.write_int(pos_val, position)
        self._out.write_short(len(field_name))
        self._out.write_from(field_name)
        self._out.write_byte(field_type)

    def end(self):
        # write final offset
        _pos_value = self._out.position()
        self._out.write_int(_pos_value, self._begin_pos)


def _check_portable_attributes(field_def, portable):
    if field_def.factory_id != portable.get_factory_id():
        raise HazelcastSerializationError("Wrong Portable type! Generic portable types are not supported! "
                                          "Expected factory-id: {}, Actual factory-id: {}"
                                          .format(field_def.factory_id, portable.get_factory_id()))
    if field_def.class_id != portable.get_class_id():
        raise HazelcastSerializationError("Wrong Portable type! Generic portable types are not supported! "
                                          "Expected class-id: {}, Actual class-id: {}"
                                          .format(field_def.class_id, portable.get_class_id()))


class ClassDefinitionWriter(PortableWriter):
    def __init__(self, portable_context, factory_id=None, class_id=None, version=None, class_def_builder=None):
        self.portable_context = portable_context
        if class_def_builder is None:
            self._builder = ClassDefinitionBuilder(factory_id, class_id, version)
        else:
            self._builder = class_def_builder

    def write_byte(self, field_name, value):
        self._builder.add_byte_field(field_name)

    def write_boolean(self, field_name, value):
        self._builder.add_boolean_field(field_name)

    def write_char(self, field_name, value):
        self._builder.add_char_field(field_name)

    def write_short(self, field_name, value):
        self._builder.add_short_field(field_name)

    def write_int(self, field_name, value):
        self._builder.add_int_field(field_name)

    def write_long(self, field_name, value):
        self._builder.add_long_field(field_name)

    def write_float(self, field_name, value):
        self._builder.add_float_field(field_name)

    def write_double(self, field_name, value):
        self._builder.add_double_field(field_name)

    def write_utf(self, field_name, value):
        self._builder.add_utf_field(field_name)

    def write_byte_array(self, field_name, values):
        self._builder.add_byte_array_field(field_name)

    def write_boolean_array(self, field_name, values):
        self._builder.add_boolean_array_field(field_name)

    def write_char_array(self, field_name, values):
        self._builder.add_char_array_field(field_name)

    def write_short_array(self, field_name, values):
        self._builder.add_short_array_field(field_name)

    def write_int_array(self, field_name, values):
        self._builder.add_int_array_field(field_name)

    def write_long_array(self, field_name, values):
        self._builder.add_long_array_field(field_name)

    def write_float_array(self, field_name, values):
        self._builder.add_float_array_field(field_name)

    def write_double_array(self, field_name, values):
        self._builder.add_double_array_field(field_name)

    def write_utf_array(self, field_name, values):
        self._builder.add_utf_array_field(field_name)

    def write_portable(self, field_name, portable):
        if portable is None:
            raise HazelcastSerializationError("Cannot write None portable without explicitly registering class definition!")
        version = util.get_portable_version(portable, self.portable_context.portable_version)

        factory_id = portable.get_factory_id()
        class_id = portable.get_class_id()
        nested_builder = ClassDefinitionBuilder(factory_id, class_id, version)
        nested_class_def = self._create_nested_class_def(portable, nested_builder)
        self._builder.add_portable_field(field_name, nested_class_def)

    def write_portable_array(self, field_name, values):
        if values is None or len(values) == 0:
            raise HazelcastSerializationError("Cannot write None portable without explicitly registering class definition!")
        portable = values[0]
        _class_id = portable.get_class_id()
        for i in xrange(1, len(values)):
            if values[i].get_class_id() != _class_id:
                raise ValueError("Detected different class-ids in portable array!")
        version = util.get_portable_version(portable, self.portable_context.portable_version)
        nested_builder = ClassDefinitionBuilder(portable.get_factory_id(), portable.get_class_id(), version)
        nested_class_def = self._create_nested_class_def(portable, nested_builder)
        self._builder.add_portable_array_field(field_name, nested_class_def)

    def write_null_portable(self, field_name, factory_id, class_id):
        nested_class_def = self.portable_context.lookup_class_definition(factory_id, class_id,
                                                                         self.portable_context.portable_version)
        if nested_class_def is None:
            raise HazelcastSerializationError("Cannot write None portable without explicitly registering class definition!")
        self._builder.add_portable_field(field_name, nested_class_def)

    def get_raw_data_output(self):
        return EmptyObjectDataOutput()

    def _create_nested_class_def(self, portable, nested_builder):
        _writer = ClassDefinitionWriter(self.portable_context, class_def_builder=nested_builder)
        portable.write_portable(_writer)
        return self.portable_context.register_class_definition(nested_builder.build())

    def register_and_get(self):
        cd = self._builder.build()
        return self.portable_context.register_class_definition(cd)
