import binascii
import time
import uuid
from datetime import datetime

from hazelcast import six
from hazelcast.core import HazelcastJsonValue
from hazelcast.serialization.bits import *
from hazelcast.serialization.api import StreamSerializer
from hazelcast.serialization.base import HazelcastSerializationError
from hazelcast.serialization.serialization_const import *
from hazelcast.six.moves import range, cPickle

from hazelcast.util import to_signed

if not six.PY2:
    long = int


class BaseSerializer(StreamSerializer):
    def destroy(self):
        pass


# DEFAULT SERIALIZERS
class NoneSerializer(BaseSerializer):
    def read(self, inp):
        return None

    def write(self, out, obj):
        pass

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
        out.write_int(obj)

    def get_type_id(self):
        return CONSTANT_TYPE_INTEGER


class LongSerializer(BaseSerializer):
    def read(self, inp):
        return inp.read_long()

    def write(self, out, obj):
        out.write_long(obj)

    def get_type_id(self):
        return CONSTANT_TYPE_LONG


class FloatSerializer(BaseSerializer):
    def read(self, inp):
        return inp.read_float()

    # "write(self, out, obj)" is never called so not implemented here

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


class UuidSerializer(BaseSerializer):
    def read(self, inp):
        msb = inp.read_long()
        lsb = inp.read_long()
        return uuid.UUID(int=(((msb << UUID_MSB_SHIFT) & UUID_MSB_MASK) | (lsb & UUID_LSB_MASK)))

    def write(self, out, obj):
        i = obj.int
        msb = to_signed(i >> UUID_MSB_SHIFT, 64)
        lsb = to_signed(i & UUID_LSB_MASK, 64)
        out.write_long(msb)
        out.write_long(lsb)

    def get_type_id(self):
        return CONSTANT_TYPE_UUID


class HazelcastJsonValueSerializer(BaseSerializer):
    def read(self, inp):
        return HazelcastJsonValue(inp.read_utf())

    def write(self, out, obj):
        out.write_utf(obj.to_string())

    def get_type_id(self):
        return JAVASCRIPT_JSON_SERIALIZATION_TYPE


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

    def write(self, out, obj):
        out.write_byte_array(obj)

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
        long_time = long(time.mktime(obj.timetuple())) * 1000
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
        the_big_int = -obj - 1 if obj < 0 else obj
        end_index = -1 if (type(obj) == long and six.PY2) else None
        hex_str = hex(the_big_int)[2:end_index]
        if len(hex_str) % 2 == 1:
            prefix = "0"  # "f" if obj < 0 else "0"
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


class JavaClassSerializer(BaseSerializer):
    def read(self, inp):
        return inp.read_utf()

    # "write(self, out, obj)" is never called so not implemented here

    def get_type_id(self):
        return JAVA_DEFAULT_TYPE_CLASS


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

    # "write(self, out, obj)" is never called so not implemented here

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
            raise HazelcastSerializationError(
                "Native clients only support IdentifiedDataSerializable!"
            )
        factory_id = inp.read_int()
        class_id = inp.read_int()

        factory = self._factories.get(factory_id, None)
        if factory is None:
            raise HazelcastSerializationError(
                "No DataSerializerFactory registered for namespace: %s" % factory_id
            )
        identified = factory.get(class_id, None)
        if identified is None:
            raise HazelcastSerializationError(
                "%s is not be able to create an instance for id: %s on factoryId: %s"
                % (factory, class_id, factory_id)
            )
        instance = identified()
        instance.read_data(inp)
        return instance

    def get_type_id(self):
        return CONSTANT_TYPE_DATA_SERIALIZABLE
