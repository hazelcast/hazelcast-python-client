from hazelcast.exception import HazelcastSerializationError
from hazelcast.util import enum

FieldType = enum(
        PORTABLE=0,
        BYTE=1,
        BOOLEAN=2,
        CHAR=3,
        SHORT=4,
        INT=5,
        LONG=6,
        FLOAT=7,
        DOUBLE=8,
        UTF=9,
        PORTABLE_ARRAY=10,
        BYTE_ARRAY=11,
        BOOLEAN_ARRAY=12,
        CHAR_ARRAY=13,
        SHORT_ARRAY=14,
        INT_ARRAY=15,
        LONG_ARRAY=16,
        FLOAT_ARRAY=17,
        DOUBLE_ARRAY=18,
        UTF_ARRAY=19
)


class FieldDefinition(object):
    def __init__(self, index, field_name, field_type, factory_id, class_id):
        self.index = index
        self.field_name = field_name
        self.field_type = field_type
        self.factory_id = factory_id
        self.class_id = class_id


class ClassDefinition(object):
    def __init__(self, factory_id, class_id, version):
        self.factory_id = factory_id
        self.class_id = class_id
        self.version = version
        self.field_defs = {}  # string:FieldDefinition

    def get_field(self, field_name_or_index):
        if isinstance(field_name_or_index, int):
            index = field_name_or_index
            count = self.get_field_count()
            if 0 <= index < count:
                for field in self.field_defs.itervalues():
                    if field.index == index:
                        return field
            raise IndexError("Index is out of bound. Index: {} and size: {}".format(index, count))
        else:
            return self.field_defs.get(field_name_or_index, None)

    def has_field(self, field_name):
        return self.field_defs.has_key(field_name)

    def get_field_names(self):
        return self.field_defs.keys()

    def get_field_type(self, field_name):
        fd = self.get_field(field_name)
        if fd:
            return fd.field_type
        raise ValueError("Unknown field: {}".format(field_name))

    def get_field_class_id(self, field_name):
        fd = self.get_field(field_name)
        if fd:
            return fd.class_id
        raise ValueError("Unknown field: {}".format(field_name))

    def get_field_count(self):
        return len(self.field_defs)


class ClassDefinitionBuilder(object):
    def __init__(self, factory_id, class_id, version=-1):
        self.factory_id = factory_id
        self.class_id = class_id
        self.version = version

        self._index = 0
        self._done = False
        self._field_defs = list()

    def add_portable_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.PORTABLE)
        return self

    def add_byte_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.BYTE)
        return self

    def add_boolean_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.BOOLEAN)
        return self

    def add_char_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.CHAR)
        return self

    def add_short_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.SHORT)
        return self

    def add_int_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.INT)
        return self

    def add_long_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.LONG)
        return self

    def add_float_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.FLOAT)
        return self

    def add_double_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.DOUBLE)
        return self

    def add_utf_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.UTF)
        return self

    def add_portable_array_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.PORTABLE_ARRAY)
        return self

    def add_byte_array_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.BYTE_ARRAY)
        return self

    def add_boolean_array_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.BOOLEAN_ARRAY)
        return self

    def add_char_array_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.CHAR_ARRAY)
        return self

    def add_short_array_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.SHORT_ARRAY)
        return self

    def add_int_array_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.INT_ARRAY)
        return self

    def add_long_array_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.LONG_ARRAY)
        return self

    def add_float_array_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.FLOAT_ARRAY)
        return self

    def add_double_array_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.DOUBLE_ARRAY)
        return self

    def add_utf_array_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.UTF_ARRAY)
        return self

    def _add_field_by_type(self, field_name, field_type):
        self._check()
        self._field_defs.append(FieldDefinition(self._index, field_name, field_type))
        self._index += 1

    def _check(self):
        if self._done:
            raise HazelcastSerializationError("ClassDefinition is already built for {}".format(self.class_id))
