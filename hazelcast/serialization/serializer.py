import datetime
import decimal
import pickle

from hazelcast.core import HazelcastJsonValue
from hazelcast.serialization.bits import *
from hazelcast.serialization.api import StreamSerializer
from hazelcast.serialization.base import HazelcastSerializationError
from hazelcast.serialization.serialization_const import *
from hazelcast.util import UUIDUtil, int_from_bytes, int_to_bytes


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
        return inp.read_string()

    def write(self, out, obj):
        out.write_string(obj)

    def get_type_id(self):
        return CONSTANT_TYPE_STRING


class UuidSerializer(BaseSerializer):
    def read(self, inp):
        msb = inp.read_long()
        lsb = inp.read_long()
        return UUIDUtil.from_bits(msb, lsb)

    def write(self, out, obj):
        msb, lsb = UUIDUtil.to_bits(obj)
        out.write_long(msb)
        out.write_long(lsb)

    def get_type_id(self):
        return CONSTANT_TYPE_UUID


class HazelcastJsonValueSerializer(BaseSerializer):
    def read(self, inp):
        return HazelcastJsonValue(inp.read_string())

    def write(self, out, obj):
        out.write_string(obj.to_string())

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
        return inp.read_string_array()

    # "write(self, out, obj)" is never called so not implemented here

    def get_type_id(self):
        return CONSTANT_TYPE_STRING_ARRAY


# EXTENSIONS
class BigIntegerSerializer(BaseSerializer):
    def read(self, inp):
        length = inp.read_int()
        result = bytearray(length)
        inp.read_into(result, 0, length)
        return int_from_bytes(result)

    def write(self, out, obj):
        out.write_byte_array(int_to_bytes(obj))

    def get_type_id(self):
        return JAVA_DEFAULT_TYPE_BIG_INTEGER


class BigDecimalSerializer(BaseSerializer):
    def read(self, inp):
        length = inp.read_int()
        result = bytearray(length)
        inp.read_into(result, 0, length)
        unscaled_value = int_from_bytes(result)
        scale = inp.read_int()
        sign = 0 if unscaled_value >= 0 else 1
        return decimal.Decimal(
            (sign, tuple(int(digit) for digit in str(abs(unscaled_value))), -1 * scale)
        )

    def write(self, out, obj):
        sign, digits, exponent = obj.as_tuple()
        unscaled_value = int("".join([str(digit) for digit in digits]))
        if sign == 1:
            unscaled_value = -1 * unscaled_value
        out.write_byte_array(int_to_bytes(unscaled_value))
        out.write_int(-1 * exponent)

    def get_type_id(self):
        return JAVA_DEFAULT_TYPE_BIG_DECIMAL


class JavaClassSerializer(BaseSerializer):
    def read(self, inp):
        return inp.read_string()

    # "write(self, out, obj)" is never called so not implemented here

    def get_type_id(self):
        return JAVA_DEFAULT_TYPE_CLASS


class ArraySerializer(BaseSerializer):
    def read(self, inp):
        size = inp.read_int()
        return [inp.read_object() for _ in range(size)]

    # "write(self, out, obj)" is never called so not implemented here

    def get_type_id(self):
        return JAVA_DEFAULT_TYPE_ARRAY


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


class LocalDateSerializer(BaseSerializer):
    def read(self, inp):
        return datetime.date(
            inp.read_int(),
            inp.read_byte(),
            inp.read_byte(),
        )

    def write(self, out, obj):
        out.write_int(obj.year)
        out.write_byte(obj.month)
        out.write_byte(obj.day)

    def get_type_id(self):
        return JAVA_DEFAULT_TYPE_LOCAL_DATE


class LocalTimeSerializer(BaseSerializer):
    def read(self, inp):
        return datetime.time(
            inp.read_byte(),
            inp.read_byte(),
            inp.read_byte(),
            inp.read_int() // 1000,  # server sends nanoseconds
        )

    def write(self, out, obj):
        out.write_byte(obj.hour)
        out.write_byte(obj.minute)
        out.write_byte(obj.second)
        out.write_int(obj.microsecond * 1000)  # server expects nanoseconds

    def get_type_id(self):
        return JAVA_DEFAULT_TYPE_LOCAL_TIME


class LocalDateTimeSerializer(BaseSerializer):
    def read(self, inp):
        return datetime.datetime(
            inp.read_int(),
            inp.read_byte(),
            inp.read_byte(),
            inp.read_byte(),
            inp.read_byte(),
            inp.read_byte(),
            inp.read_int() // 1000,  # server sends nanoseconds
        )

    # "write(self, out, obj)" is never called so not implemented here

    def get_type_id(self):
        return JAVA_DEFAULT_TYPE_LOCAL_DATE_TIME


class OffsetDateTimeSerializer(BaseSerializer):
    def read(self, inp):
        return datetime.datetime(
            inp.read_int(),
            inp.read_byte(),
            inp.read_byte(),
            inp.read_byte(),
            inp.read_byte(),
            inp.read_byte(),
            inp.read_int() // 1000,  # server sends nanoseconds
            datetime.timezone(datetime.timedelta(seconds=inp.read_int())),
        )

    def write(self, out, obj):
        out.write_int(obj.year)
        out.write_byte(obj.month)
        out.write_byte(obj.day)
        out.write_byte(obj.hour)
        out.write_byte(obj.minute)
        out.write_byte(obj.second)
        out.write_int(obj.microsecond * 1000)  # server expects nanoseconds

        timezone_info = obj.tzinfo
        if not timezone_info:
            out.write_int(0)
            return

        utc_offset = timezone_info.utcoffset(None)
        if utc_offset:
            out.write_int(int(utc_offset.total_seconds()))
        else:
            out.write_int(0)

    def get_type_id(self):
        return JAVA_DEFAULT_TYPE_OFFSET_DATE_TIME


class PythonObjectSerializer(BaseSerializer):
    def read(self, inp):
        str = inp.read_string().encode()
        return pickle.loads(str)

    def write(self, out, obj):
        out.write_string(pickle.dumps(obj, 0).decode("utf-8"))

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
