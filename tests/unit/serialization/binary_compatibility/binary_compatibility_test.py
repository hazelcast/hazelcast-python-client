import itertools
import unittest

from os import path
from parameterized import parameterized

from hazelcast.config import Config
from hazelcast.number_types import BigInt, Int8, Int16, Int64, Float32
from hazelcast.serialization import BE_INT, BE_FLOAT, SerializationServiceV1
from hazelcast.serialization.api import StreamSerializer
from hazelcast.serialization.input import _ObjectDataInput
from hazelcast.serialization.portable.classdef import ClassDefinitionBuilder
from tests.unit.serialization.binary_compatibility.reference_objects import *

NULL_LENGTH = -1
IS_BIG_ENDIAN = [True, False]
FILE_NAME = "1.serialization.compatibility.binary"


class BinaryCompatibilityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        data_map = dict()
        here = path.abspath(path.dirname(__file__))
        with open(path.join(here, FILE_NAME), "rb") as f:
            inp = _ObjectDataInput(f.read())

        while inp.position() < inp.size():
            utf_len = inp.read_unsigned_short()
            object_key_buf = bytearray(utf_len)
            inp.read_into(object_key_buf)
            object_key = object_key_buf.decode()
            n = inp.read_int()
            if n != NULL_LENGTH:
                buf = bytearray(n)
                inp.read_into(buf)
                data_map[object_key] = Data(buf)
            else:
                data_map[object_key] = None

        cls.data_map = data_map

    @parameterized.expand(
        map(
            lambda x: ("%s_is_big_endian=%s" % (x[0], x[1]), x[0], x[1]),
            itertools.product(REFERENCE_OBJECTS.keys(), IS_BIG_ENDIAN),
        )
    )
    def test_serialize(self, _, name, is_big_endian):
        if skip_on_serialize(name):
            return

        ss = self._create_serialization_service(is_big_endian)
        object_key = self._create_object_key(name, is_big_endian)
        from_binary = self.data_map[object_key]
        value = REFERENCE_OBJECTS[name]
        match name:
            case "Byte":
                value = Int8(value)
            case "Short":
                value = Int16(value)
            case "Long":
                value = Int64(value)
            case "BigInteger":
                value = BigInt(value)
            case "Float":
                value = Float32(value)
        serialized = ss.to_data(value)
        self.assertEqual(from_binary, serialized)

    @parameterized.expand(
        map(
            lambda x: ("%s_is_big_endian=%s" % (x[0], x[1]), x[0], x[1]),
            itertools.product(REFERENCE_OBJECTS.keys(), IS_BIG_ENDIAN),
        )
    )
    def test_deserialize(self, _, name, is_big_endian):
        if skip_on_deserialize(name):
            return

        ss = self._create_serialization_service(is_big_endian)
        object_key = self._create_object_key(name, is_big_endian)
        from_binary = self.data_map[object_key]
        deserialized = ss.to_object(from_binary)
        obj = REFERENCE_OBJECTS[name]
        # bool is an instance of int, so need to exclude that specifically --YT
        if not isinstance(deserialized, bool) and isinstance(deserialized, int):
            obj = int(obj)
        self.assertTrue(is_equal(obj, deserialized))

    @parameterized.expand(
        map(
            lambda x: ("%s_is_big_endian=%s" % (x[0], x[1]), x[0], x[1]),
            itertools.product(REFERENCE_OBJECTS.keys(), IS_BIG_ENDIAN),
        )
    )
    def test_serialize_deserialize(self, _, name, is_big_endian):
        if skip_on_deserialize(name) or skip_on_serialize(name):
            return

        ss = self._create_serialization_service(is_big_endian)
        obj = REFERENCE_OBJECTS[name]
        if name == "BigInteger":
            data = ss.to_data(BigInt(obj))
        else:
            data = ss.to_data(obj)
        deserialized = ss.to_object(data)
        self.assertTrue(is_equal(obj, deserialized))

    @staticmethod
    def _convert_to_byte_order(is_big_endian):
        if is_big_endian:
            return "BIG_ENDIAN"
        return "LITTLE_ENDIAN"

    @staticmethod
    def _create_object_key(name, is_big_endian):
        return "1-%s-%s" % (name, BinaryCompatibilityTest._convert_to_byte_order(is_big_endian))

    @staticmethod
    def _create_serialization_service(is_big_endian):
        config = Config()
        config.custom_serializers = {
            CustomStreamSerializable: CustomStreamSerializer,
            CustomByteArraySerializable: CustomByteArraySerializer,
        }
        config.is_big_endian = is_big_endian
        cdb = ClassDefinitionBuilder(PORTABLE_FACTORY_ID, INNER_PORTABLE_CLASS_ID)
        cdb.add_int_field("i")
        cdb.add_float_field("f")
        cd = cdb.build()
        config.class_definitions = [cd]
        config.portable_factories = {
            PORTABLE_FACTORY_ID: {
                PORTABLE_CLASS_ID: APortable,
                INNER_PORTABLE_CLASS_ID: AnInnerPortable,
            }
        }
        config.data_serializable_factories = {
            IDENTIFIED_DATA_SERIALIZABLE_FACTORY_ID: {
                DATA_SERIALIZABLE_CLASS_ID: AnIdentifiedDataSerializable
            }
        }
        return SerializationServiceV1(config)


class CustomStreamSerializer(StreamSerializer):
    def write(self, out, obj):
        out.write_int(obj.i)
        out.write_float(obj.f)

    def read(self, inp):
        return CustomStreamSerializable(inp.read_int(), inp.read_float())

    def get_type_id(self):
        return CUSTOM_STREAM_SERIALIZABLE_ID

    def destroy(self):
        pass


class CustomByteArraySerializer(StreamSerializer):
    def write(self, out, obj):
        buf = bytearray(10)
        BE_INT.pack_into(buf, 0, obj.i)
        BE_FLOAT.pack_into(buf, 4, obj.f)
        out.write_byte_array(buf)

    def read(self, inp):
        buf = inp.read_byte_array()
        return CustomByteArraySerializable(
            BE_INT.unpack_from(buf, 0)[0], BE_FLOAT.unpack_from(buf, 4)[0]
        )

    def get_type_id(self):
        return CUSTOM_BYTE_ARRAY_SERIALIZABLE_ID

    def destroy(self):
        pass
