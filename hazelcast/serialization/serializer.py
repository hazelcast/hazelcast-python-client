import cPickle
from datetime import datetime
from time import time

from bits import *
from hazelcast.serialization.api import StreamSerializer
# from hazelcast.serialization.base import HazelcastSerializationError
from hazelcast.serialization.serialization_const import *


# DEFAULT SERIALIZERS
class NoneSerializer(StreamSerializer):
    def read(self, inp):
        return None

    def write(self, out, obj):
        pass

    def get_type_id(self):
        return CONSTANT_TYPE_NULL


class BooleanSerializer(StreamSerializer):
    def read(self, inp):
        return inp.read_boolean()

    def write(self, out, obj):
        out.write_boolean(obj)

    def get_type_id(self):
        return CONSTANT_TYPE_BOOLEAN


class ByteSerializer(StreamSerializer):
    def read(self, inp):
        return inp.read_byte()

    def write(self, out, obj):
        out.write_byte(obj)

    def get_type_id(self):
        return CONSTANT_TYPE_BYTE


class CharSerializer(StreamSerializer):
    def read(self, inp):
        return inp.read_char()

    def write(self, out, obj):
        out.write_char(obj)

    def get_type_id(self):
        return CONSTANT_TYPE_CHAR


class ShortSerializer(StreamSerializer):
    def read(self, inp):
        return inp.read_short()

    def write(self, out, obj):
        out.write_short(obj)

    def get_type_id(self):
        return CONSTANT_TYPE_SHORT


class IntegerSerializer(StreamSerializer):
    def read(self, inp):
        return inp.read_int()

    def write(self, out, obj):
        if obj.bit_length() < 32:
            out.write_int(obj)
        else:
            raise ValueError("Serialization only supports 32 bit ints")

    def get_type_id(self):
        return CONSTANT_TYPE_INTEGER


class LongSerializer(StreamSerializer):
    def read(self, inp):
        return inp.read_long()

    def write(self, out, obj):
        if obj.bit_length() < 64:
            out.write_long(obj)
        else:
            raise ValueError("Serialization only supports 64 bit longs")

    def get_type_id(self):
        return CONSTANT_TYPE_LONG


class FloatSerializer(StreamSerializer):
    def read(self, inp):
        return inp.read_float()

    def write(self, out, obj):
        out.write_float(obj)

    def get_type_id(self):
        return CONSTANT_TYPE_FLOAT


class DoubleSerializer(StreamSerializer):
    def read(self, inp):
        return inp.read_double()

    def write(self, out, obj):
        out.write_double(obj)

    def get_type_id(self):
        return CONSTANT_TYPE_DOUBLE


class StringSerializer(StreamSerializer):
    def read(self, inp):
        return inp.read_utf()

    def write(self, out, obj):
        out.write_utf(obj)

    def get_type_id(self):
        return CONSTANT_TYPE_STRING


# ARRAY SERIALIZERS
class BooleanArraySerializer(StreamSerializer):
    def read(self, inp):
        return inp.read_boolean_array()

    def write(self, out, obj):
        out.write_boolean_array(obj)

    def get_type_id(self):
        return CONSTANT_TYPE_BOOLEAN_ARRAY


class ByteArraySerializer(StreamSerializer):
    def read(self, inp):
        return inp.read_byte_array()

    def write(self, out, obj):
        out.write_byte_array(obj)

    def get_type_id(self):
        return CONSTANT_TYPE_BYTE_ARRAY


class CharArraySerializer(StreamSerializer):
    def read(self, inp):
        return inp.read_char_array()

    def write(self, out, obj):
        out.write_char_array(obj)

    def get_type_id(self):
        return CONSTANT_TYPE_CHAR_ARRAY


class ShortArraySerializer(StreamSerializer):
    def read(self, inp):
        return inp.read_short_array()

    def write(self, out, obj):
        out.write_short_array(obj)

    def get_type_id(self):
        return CONSTANT_TYPE_SHORT_ARRAY


class IntegerArraySerializer(StreamSerializer):
    def read(self, inp):
        return inp.read_int_array()

    def write(self, out, obj):
        out.write_int_array(obj)

    def get_type_id(self):
        return CONSTANT_TYPE_INTEGER_ARRAY


class LongArraySerializer(StreamSerializer):
    def read(self, inp):
        return inp.read_long_array()

    def write(self, out, obj):
        out.write_long_array(obj)

    def get_type_id(self):
        return CONSTANT_TYPE_LONG_ARRAY


class FloatArraySerializer(StreamSerializer):
    def read(self, inp):
        return inp.read_float_array()

    def write(self, out, obj):
        out.write_float_array(obj)

    def get_type_id(self):
        return CONSTANT_TYPE_FLOAT_ARRAY


class DoubleArraySerializer(StreamSerializer):
    def read(self, inp):
        return inp.read_double_array()

    def write(self, out, obj):
        out.write_double_array(obj)

    def get_type_id(self):
        return CONSTANT_TYPE_DOUBLE_ARRAY


class StringArraySerializer(StreamSerializer):
    def read(self, inp):
        return inp.read_utf_array()

    def write(self, out, obj):
        out.write_utf_array(obj)

    def get_type_id(self):
        return CONSTANT_TYPE_STRING_ARRAY


# EXTENSIONS
class DateTimeSerializer(StreamSerializer):
    def read(self, inp):
        long_time = inp.read_long()
        return datetime.fromtimestamp(long_time / 1000.0)

    def write(self, out, obj):
        long_time = long(time.mktime(obj.timetuple()))
        out.write_long(long_time)

    def get_type_id(self):
        return JAVA_DEFAULT_TYPE_DATE


class BigIntegerSerializer(StreamSerializer):
    def read(self, inp):
        raise NotImplementedError("Big integer numbers not supported")

    def write(self, out, obj):
        raise NotImplementedError("Big integer numbers not supported")

    def get_type_id(self):
        return JAVA_DEFAULT_TYPE_BIG_INTEGER


class BigDecimalSerializer(StreamSerializer):
    def read(self, inp):
        raise NotImplementedError("Big decimal numbers not supported")

    def write(self, out, obj):
        raise NotImplementedError("Big decimal numbers not supported")

    def get_type_id(self):
        return JAVA_DEFAULT_TYPE_BIG_DECIMAL


class JavaClassSerializer(StreamSerializer):
    def read(self, inp):
        return inp.read_utf()

    def write(self, out, obj):
        out.write_utf(obj)

    def get_type_id(self):
        return JAVA_DEFAULT_TYPE_CLASS


class JavaEnumSerializer(StreamSerializer):
    def read(self, inp):
        """
        :param inp:
        :return: a tuple of (Enum-name, Enum-value-name)
        """
        return tuple(inp.read_utf(), inp.read_utf())

    def write(self, out, obj):
        enum_name, enum_val_name = obj
        out.write_utf(enum_name)
        out.write_utf(enum_val_name)

    def get_type_id(self):
        return JAVA_DEFAULT_TYPE_ENUM


class ArrayListSerializer(StreamSerializer):
    def read(self, inp):
        size = inp.read_int()
        if size > NULL_ARRAY_LENGTH:
            return [inp.read_object() for _ in xrange(0, size)]
        return None

    def write(self, out, obj):
        size = NULL_ARRAY_LENGTH if obj is None else len(obj)
        out.write_int(size)
        for i in xrange(0, size):
            out.write_object(obj[i])

    def get_type_id(self):
        return JAVA_DEFAULT_TYPE_ARRAY_LIST


class LinkedListSerializer(StreamSerializer):
    def read(self, inp):
        size = inp.read_int()
        if size > NULL_ARRAY_LENGTH:
            return [inp.read_object() for _ in xrange(0, size)]
        return None

    def write(self, out, obj):
        raise NotImplementedError("writing Link lists not supported")

    def get_type_id(self):
        return JAVA_DEFAULT_TYPE_LINKED_LIST


class PythonObjectSerializer(StreamSerializer):
    def read(self, inp):
        str = inp.read_utf().encode()
        return cPickle.loads(str)

    def write(self, out, obj):
        out.write_utf(cPickle.dumps(obj))

    def get_type_id(self):
        return PYTHON_TYPE_PICKLE


class IdentifiedDataSerializer(StreamSerializer):
    def __init__(self, factories):
        self._factories = factories

    def write(self, out, obj):
        out.write_boolean(True)  # Always identified
        out.write_int(obj.get_factory_id())
        out.write_int(obj.get_class_id())
        obj.write_data(out)

    def read(self, inp):
        is_identified = inp.read_boolean()
        if not is_identified:
            raise HazelcastSerializationError("Native clients only support IdentifiedDataSerializable!")
        factory_id = inp.read_int()
        class_id = inp.read_int()

        factory = self._factories.get(factory_id, None)
        if factory is None:
            from hazelcast.serialization.base import HazelcastSerializationError
            raise HazelcastSerializationError("No DataSerializerFactory registered for namespace: {}".format(factory_id))
        identified = factory.get(class_id, None)
        if identified is None:
            raise HazelcastSerializationError(
                    "{} is not be able to create an instance for id: {} on factoryId: {}".format(factory, class_id, factory_id))
        instance = identified()
        instance.read_data(inp)
        return instance

    def get_type_id(self):
        return CONSTANT_TYPE_DATA_SERIALIZABLE
