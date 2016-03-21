import unittest

from hazelcast.config import SerializationConfig, INTEGER_TYPE
from hazelcast.serialization.serialization_const import CONSTANT_TYPE_BYTE, CONSTANT_TYPE_SHORT, CONSTANT_TYPE_INTEGER, \
    CONSTANT_TYPE_LONG
from hazelcast.serialization.service import SerializationServiceV1
from tests.util import random_string


class SerializationTestCase(unittest.TestCase):
    def setUp(self):
        n = random_string()
        self.service = SerializationServiceV1(serialization_config=SerializationConfig())

    def test_test_dummy_encode_decode(self):
        obj = "Test obj"
        data = self.service.to_data(obj)

        obj2 = self.service.to_object(data)
        self.assertEqual(obj, obj2)

    def test_service_int(self):
        obj = 10
        data = self.service.to_data(obj)

        obj2 = self.service.to_object(data)
        self.assertEqual(obj, obj2)
        self.assertEqual(type(obj), type(obj2))

    def test_service_int_array(self):
        obj = [10, 20, 30]
        data = self.service.to_data(obj)

        obj2 = self.service.to_object(data)
        self.assertEqual(obj, obj2)

    def test_service_data(self):
        obj = "TEST"
        data = self.service.to_data(obj)

        self.assertEqual(16, len(data.to_bytes()))


byte_val = 0x12
short_val = 0x1234
int_val = 0x12345678
long_val = 0x1122334455667788


class IntegerTestCase(unittest.TestCase):
    def test_dynamic_case(self):
        config = SerializationConfig()
        config.default_integer_type = INTEGER_TYPE.VAR
        service = SerializationServiceV1(serialization_config=config)

        d1 = service.to_data(byte_val)
        d2 = service.to_data(short_val)
        d3 = service.to_data(int_val)
        d4 = service.to_data(long_val)

        self.assertEqual(d1.get_type(), CONSTANT_TYPE_BYTE)
        self.assertEqual(d2.get_type(), CONSTANT_TYPE_SHORT)
        self.assertEqual(d3.get_type(), CONSTANT_TYPE_INTEGER)
        self.assertEqual(d4.get_type(), CONSTANT_TYPE_LONG)

    def test_byte_case(self):
        config = SerializationConfig()
        config.default_integer_type = INTEGER_TYPE.BYTE
        service = SerializationServiceV1(serialization_config=config)

        d1 = service.to_data(byte_val)

        self.assertEqual(d1.get_type(), CONSTANT_TYPE_BYTE)

    def test_short_case(self):
        config = SerializationConfig()
        config.default_integer_type = INTEGER_TYPE.SHORT
        service = SerializationServiceV1(serialization_config=config)

        d1 = service.to_data(byte_val)
        d2 = service.to_data(short_val)

        self.assertEqual(d1.get_type(), CONSTANT_TYPE_SHORT)
        self.assertEqual(d2.get_type(), CONSTANT_TYPE_SHORT)

    def test_int_case(self):
        config = SerializationConfig()
        config.default_integer_type = INTEGER_TYPE.INT
        service = SerializationServiceV1(serialization_config=config)

        d1 = service.to_data(byte_val)
        d2 = service.to_data(short_val)
        d3 = service.to_data(int_val)

        self.assertEqual(d1.get_type(), CONSTANT_TYPE_INTEGER)
        self.assertEqual(d2.get_type(), CONSTANT_TYPE_INTEGER)
        self.assertEqual(d3.get_type(), CONSTANT_TYPE_INTEGER)

    def test_long_case(self):
        config = SerializationConfig()
        config.default_integer_type = INTEGER_TYPE.LONG
        service = SerializationServiceV1(serialization_config=config)

        d1 = service.to_data(byte_val)
        d2 = service.to_data(short_val)
        d3 = service.to_data(int_val)
        d4 = service.to_data(long_val)

        self.assertEqual(d1.get_type(), CONSTANT_TYPE_LONG)
        self.assertEqual(d2.get_type(), CONSTANT_TYPE_LONG)
        self.assertEqual(d3.get_type(), CONSTANT_TYPE_LONG)
        self.assertEqual(d4.get_type(), CONSTANT_TYPE_LONG)
