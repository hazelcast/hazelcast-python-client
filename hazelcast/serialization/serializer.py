import binascii
from datetime import datetime
from time import time

from hazelcast.serialization.bits import *
from hazelcast.serialization.api import StreamSerializer
from hazelcast.serialization.base import HazelcastSerializationError
from hazelcast.serialization.serialization_const import *
from hazelcast import six
from hazelcast.six.moves import range, cPickle

if not six.PY2:
    long = int


class BaseSerializer(StreamSerializer):
    def destroy(self):
        pass


# DEFAULT SERIALIZERS
class NoneSerializer(BaseSerializer):
    def read(self, inp):
        return None

    # "write(self, out, obj)" is never called so not implemented here

    def get_type_id(self):
        return CONSTANT_TYPE_NULL


class BooleanSerializer(BaseSerializer):
    def read(self, inp):
        return inp.read_boolean()

    def write(self, out, obj):
        out.write_boolean(obj)

    def get_type_id(self):
        return CONSTANT_TYPE_BOOLEAN


class ByteSerializer(BaseSerializer):
    def read(self, inp):
        return inp.read_byte()

    def write(self, out, obj):
        out.write_byte(obj)

    def get_type_id(self):
        return CONSTANT_TYPE_BYTE


class CharSerializer(BaseSerializer):
    def read(self, inp):
        return inp.read_char()

    # "write(self, out, obj)" is never called so not implemented here

    def get_type_id(self):
        return CONSTANT_TYPE_CHAR


class ShortSerializer(BaseSerializer):
    def read(self, inp):
        return inp.read_short()

    def write(self, out, obj):
        out.write_short(obj)

    def get_type_id(self):
        return CONSTANT_TYPE_SHORT


class IntegerSerializer(BaseSerializer):
    def read(self, inp):
        return inp.read_int()

    def write(self, out, obj):
        if obj.bit_length() < 32:
            out.write_int(obj)
        else:
            raise ValueError("Serialization only supports 32 bit ints")

    def get_type_id(self):
        return CONSTANT_TYPE_INTEGER


class LongSerializer(BaseSerializer):
    def read(self, inp):
        return inp.read_long()

    def write(self, out, obj):
        if obj.bit_length() < 64:
            out.write_long(obj)
        else:
            raise ValueError("Serialization only supports 64 bit longs")

    def get_type_id(self):
        return CONSTANT_TYPE_LONG


class FloatSerializer(BaseSerializer):
    def read(self, inp):
        return inp.read_float()

    def write(self, out, obj):
        out.write_float(obj)

    def get_type_id(self):
        return CONSTANT_TYPE_FLOAT


class DoubleSerializer(BaseSerializer):
    def read(self, inp):
        return inp.read_double()

    def write(self, out, obj):
        out.write_double(obj)

    def get_type_id(self):
        return CONSTANT_TYPE_DOUBLE


class StringSerializer(BaseSerializer):
    def read(self, inp):
        return inp.read_utf()

    def write(self, out, obj):
        out.write_utf(obj)

    def get_type_id(self):
        return CONSTANT_TYPE_STRING


# ARRAY SERIALIZERS
class BooleanArraySerializer(BaseSerializer):
    def read(self, inp):
        return inp.read_boolean_array()

    # "write(self, out, obj)" is never called so not implemented here

    def get_type_id(self):
        return CONSTANT_TYPE_BOOLEAN_ARRAY


class ByteArraySerializer(BaseSerializer):
    def read(self, inp):
        return inp.read_byte_array()

    # "write(self, out, obj)" is never called so not implemented here

    def get_type_id(self):
        return CONSTANT_TYPE_BYTE_ARRAY


class CharArraySerializer(BaseSerializer):
    def read(self, inp):
        return inp.read_char_array()

    # "write(self, out, obj)" is never called so not implemented here

    def get_type_id(self):
        return CONSTANT_TYPE_CHAR_ARRAY


class ShortArraySerializer(BaseSerializer):
    def read(self, inp):
        return inp.read_short_array()

    # "write(self, out, obj)" is never called so not implemented here

    def get_type_id(self):
        return CONSTANT_TYPE_SHORT_ARRAY


class IntegerArraySerializer(BaseSerializer):
    def read(self, inp):
        return inp.read_int_array()

    # "write(self, out, obj)" is never called so not implemented here

    def get_type_id(self):
        return CONSTANT_TYPE_INTEGER_ARRAY


class LongArraySerializer(BaseSerializer):
    def read(self, inp):
        return inp.read_long_array()

    # "write(self, out, obj)" is never called so not implemented here

    def get_type_id(self):
        return CONSTANT_TYPE_LONG_ARRAY


class FloatArraySerializer(BaseSerializer):
    def read(self, inp):
        return inp.read_float_array()

    # "write(self, out, obj)" is never called so not implemented here

    def get_type_id(self):
        return CONSTANT_TYPE_FLOAT_ARRAY


class DoubleArraySerializer(BaseSerializer):
    def read(self, inp):
        return inp.read_double_array()

    # "write(self, out, obj)" is never called so not implemented here

    def get_type_id(self):
        return CONSTANT_TYPE_DOUBLE_ARRAY


class StringArraySerializer(BaseSerializer):
    def read(self, inp):
        return inp.read_utf_array()

    # "write(self, out, obj)" is never called so not implemented here

    def get_type_id(self):
        return CONSTANT_TYPE_STRING_ARRAY


# EXTENSIONS
class DateTimeSerializer(BaseSerializer):
    def read(self, inp):
        long_time = inp.read_long()
        return datetime.fromtimestamp(long_time / 1000.0)

    def write(self, out, obj):
        long_time = long(time.mktime(obj.timetuple()))
        out.write_long(long_time)

    def get_type_id(self):
        return JAVA_DEFAULT_TYPE_DATE


class BigIntegerSerializer(BaseSerializer):
    def read(self, inp):
        length = inp.read_int()
        if length == NULL_ARRAY_LENGTH:
            return None
        result = bytearray(length)
        if length > 0:
            inp.read_into(result, 0, length)
        if result[0] & 0x80:
            neg = bytearray()
            for c in result:
                neg.append(c ^ 0xFF)
            return -1 * int(binascii.hexlify(neg), 16) - 1
        return int(binascii.hexlify(result), 16)

    def write(self, out, obj):
        the_big_int = -obj-1 if obj < 0 else obj
        end_index = -1 if (type(obj) == long and six.PY2) else None
        hex_str = hex(the_big_int)[2:end_index]
        if len(hex_str) % 2 == 1:
            prefix = '0' # "f" if obj < 0 else "0"
            hex_str = prefix + hex_str
        num_array = bytearray(binascii.unhexlify(bytearray(hex_str, encoding="utf-8")))
        if obj < 0:
            neg = bytearray()
            for c in num_array:
                neg.append(c ^ 0xFF)
            num_array = neg
        out.write_byte_array(num_array)

    def get_type_id(self):
        return JAVA_DEFAULT_TYPE_BIG_INTEGER


class BigDecimalSerializer(BaseSerializer):
    def read(self, inp):
        raise NotImplementedError("Big decimal numbers not supported")

    def write(self, out, obj):
        raise NotImplementedError("Big decimal numbers not supported")

    def get_type_id(self):
        return JAVA_DEFAULT_TYPE_BIG_DECIMAL


class JavaClassSerializer(BaseSerializer):
    def read(self, inp):
        return inp.read_utf()

    def write(self, out, obj):
        out.write_utf(obj)

    def get_type_id(self):
        return JAVA_DEFAULT_TYPE_CLASS


class JavaEnumSerializer(BaseSerializer):
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


class ArrayListSerializer(BaseSerializer):
    def read(self, inp):
        size = inp.read_int()
        if size > NULL_ARRAY_LENGTH:
            return [inp.read_object() for _ in range(0, size)]
        return None

    def write(self, out, obj):
        size = NULL_ARRAY_LENGTH if obj is None else len(obj)
        out.write_int(size)
        for i in range(0, size):
            out.write_object(obj[i])

    def get_type_id(self):
        return JAVA_DEFAULT_TYPE_ARRAY_LIST


class LinkedListSerializer(BaseSerializer):
    def read(self, inp):
        size = inp.read_int()
        if size > NULL_ARRAY_LENGTH:
            return [inp.read_object() for _ in range(0, size)]
        return None

    def write(self, out, obj):
        raise NotImplementedError("writing Link lists not supported")

    def get_type_id(self):
        return JAVA_DEFAULT_TYPE_LINKED_LIST


class PythonObjectSerializer(BaseSerializer):
    def read(self, inp):
        str = inp.read_utf().encode()
        return cPickle.loads(str)

    def write(self, out, obj):
        out.write_utf(cPickle.dumps(obj, 0).decode("utf-8"))

    def get_type_id(self):
        return PYTHON_TYPE_PICKLE


class IdentifiedDataSerializer(BaseSerializer):
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
