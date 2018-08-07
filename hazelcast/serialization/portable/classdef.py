from hazelcast.exception import HazelcastSerializationError
from hazelcast.util import enum
from hazelcast import six

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
    def __init__(self, index, field_name, field_type, version, factory_id=0, class_id=0):
        self.index = index
        self.field_name = field_name
        self.field_type = field_type
        self.version = version
        self.factory_id = factory_id
        self.class_id = class_id

    def __eq__(self, other):
        return isinstance(other, self.__class__) \
               and (self.index, self.field_name, self.field_type, self.version, self.factory_id, self.class_id) == \
                   (other.index, other.field_name, other.field_type, other.version, other.factory_id, other.class_id)

    def __repr__(self):
        return "FieldDefinition[ ix:{}, name:{}, type:{}, version:{}, fid:{}, cid:{}]".format(self.index,
                                                                                              self.field_name,
                                                                                              self.field_type,
                                                                                              self.version,
                                                                                              self.factory_id,
                                                                                              self.class_id)


class ClassDefinition(object):
    def __init__(self, factory_id, class_id, version):
        self.factory_id = factory_id
        self.class_id = class_id
        self.version = version
        self.field_defs = {}  # string:FieldDefinition

    def add_field_def(self, field_def):
        self.field_defs[field_def.field_name] = field_def

    def get_field(self, field_name_or_index):
        if isinstance(field_name_or_index, int):
            index = field_name_or_index
            count = self.get_field_count()
            if 0 <= index < count:
                for field in six.itervalues(self.field_defs):
                    if field.index == index:
                        return field
            raise IndexError("Index is out of bound. Index: {} and size: {}".format(index, count))
        else:
            return self.field_defs.get(field_name_or_index, None)

    def has_field(self, field_name):
        return field_name in self.field_defs

    def get_field_names(self):
        return list(self.field_defs.keys())

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

    def set_version_if_not_set(self, version):
        if self.version < 0:
            self.version = version

    def __eq__(self, other):
        return isinstance(other, self.__class__) and (self.factory_id, self.class_id, self.version, self.field_defs) == \
                                                     (other.factory_id, other.class_id, other.version, other.field_defs)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return "fid:{}, cid:{}, v:{}, fields:{}".format(self.factory_id, self.class_id, self.version, self.field_defs)

    def __hash__(self):
        return id(self)//16


class ClassDefinitionBuilder(object):
    def __init__(self, factory_id, class_id, version=0):
        self.factory_id = factory_id
        self.class_id = class_id
        self.version = version

        self._index = 0
        self._done = False
        self._field_defs = list()

    def add_portable_field(self, field_name, class_def):
        if class_def.class_id is None or class_def.class_id == 0:
            raise ValueError("Portable class id cannot be zero!")
        self._add_field_by_type(field_name, FieldType.PORTABLE, class_def.version,
                                class_def.factory_id, class_def.class_id)
        return self

    def add_byte_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.BYTE, self.version)
        return self

    def add_boolean_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.BOOLEAN, self.version)
        return self

    def add_char_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.CHAR, self.version)
        return self

    def add_short_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.SHORT, self.version)
        return self

    def add_int_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.INT, self.version)
        return self

    def add_long_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.LONG, self.version)
        return self

    def add_float_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.FLOAT, self.version)
        return self

    def add_double_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.DOUBLE, self.version)
        return self

    def add_utf_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.UTF, self.version)
        return self

    def add_portable_array_field(self, field_name, class_def):
        if class_def.class_id is None or class_def.class_id == 0:
            raise ValueError("Portable class id cannot be zero!")
        self._add_field_by_type(field_name, FieldType.PORTABLE_ARRAY, class_def.version,
                                class_def.factory_id, class_def.class_id)
        return self

    def add_byte_array_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.BYTE_ARRAY, self.version)
        return self

    def add_boolean_array_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.BOOLEAN_ARRAY, self.version)
        return self

    def add_char_array_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.CHAR_ARRAY, self.version)
        return self

    def add_short_array_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.SHORT_ARRAY, self.version)
        return self

    def add_int_array_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.INT_ARRAY, self.version)
        return self

    def add_long_array_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.LONG_ARRAY, self.version)
        return self

    def add_float_array_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.FLOAT_ARRAY, self.version)
        return self

    def add_double_array_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.DOUBLE_ARRAY, self.version)
        return self

    def add_utf_array_field(self, field_name):
        self._add_field_by_type(field_name, FieldType.UTF_ARRAY, self.version)
        return self

    def add_field_def(self, field_def):
        self._check()
        if self._index != field_def.index:
            raise ValueError("Invalid field index")
        self._index += 1
        self._field_defs.append(field_def)
        return self

    def build(self):
        self._done = True
        cd = ClassDefinition(self.factory_id, self.class_id, self.version)
        for field_def in self._field_defs:
            cd.add_field_def(field_def)
        return cd

    def _add_field_by_type(self, field_name, field_type, version, factory_id=0, class_id=0):
        self._check()
        self._field_defs.append(FieldDefinition(self._index, field_name, field_type, version, factory_id, class_id))
        self._index += 1

    def _check(self):
        if self._done:
            raise HazelcastSerializationError("ClassDefinition is already built for {}".format(self.class_id))
