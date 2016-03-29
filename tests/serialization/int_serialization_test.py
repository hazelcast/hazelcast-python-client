import unittest

from hazelcast.config import SerializationConfig, INTEGER_TYPE
from hazelcast.exception import HazelcastSerializationError
from hazelcast.serialization.serialization_const import CONSTANT_TYPE_BYTE, CONSTANT_TYPE_SHORT, CONSTANT_TYPE_INTEGER, \
    CONSTANT_TYPE_LONG
from hazelcast.serialization.service import SerializationServiceV1

byte_val = 0x12
short_val = 0x1234
int_val = 0x12345678
long_val = 0x1122334455667788
big_int = 0x11223344556677881122334455667788


class IntegerTestCase(unittest.TestCase):
    def test_dynamic_case(self):
        config = SerializationConfig()
        config.default_integer_type = INTEGER_TYPE.VAR
        service = SerializationServiceV1(serialization_config=config)

        d1 = service.to_data(byte_val)
        d2 = service.to_data(short_val)
        d3 = service.to_data(int_val)
        d4 = service.to_data(long_val)
        v1 = service.to_object(d1)
        v2 = service.to_object(d2)
        v3 = service.to_object(d3)
        v4 = service.to_object(d4)

        self.assertEqual(d1.get_type(), CONSTANT_TYPE_BYTE)
        self.assertEqual(d2.get_type(), CONSTANT_TYPE_SHORT)
        self.assertEqual(d3.get_type(), CONSTANT_TYPE_INTEGER)
        self.assertEqual(d4.get_type(), CONSTANT_TYPE_LONG)
        self.assertEqual(v1, byte_val)
        self.assertEqual(v2, short_val)
        self.assertEqual(v3, int_val)
        self.assertEqual(v4, long_val)

    def test_byte_case(self):
        config = SerializationConfig()
        config.default_integer_type = INTEGER_TYPE.BYTE
        service = SerializationServiceV1(serialization_config=config)

        d1 = service.to_data(byte_val)
        v1 = service.to_object(d1)

        self.assertEqual(d1.get_type(), CONSTANT_TYPE_BYTE)
        self.assertEqual(v1, byte_val)
        with self.assertRaises(HazelcastSerializationError):
            service.to_data(big_int)

    def test_short_case(self):
        config = SerializationConfig()
        config.default_integer_type = INTEGER_TYPE.SHORT
        service = SerializationServiceV1(serialization_config=config)

        d1 = service.to_data(byte_val)
        d2 = service.to_data(short_val)
        v1 = service.to_object(d1)
        v2 = service.to_object(d2)

        self.assertEqual(d1.get_type(), CONSTANT_TYPE_SHORT)
        self.assertEqual(d2.get_type(), CONSTANT_TYPE_SHORT)
        self.assertEqual(v1, byte_val)
        self.assertEqual(v2, short_val)
        with self.assertRaises(HazelcastSerializationError):
            service.to_data(big_int)

    def test_int_case(self):
        config = SerializationConfig()
        config.default_integer_type = INTEGER_TYPE.INT
        service = SerializationServiceV1(serialization_config=config)

        d1 = service.to_data(byte_val)
        d2 = service.to_data(short_val)
        d3 = service.to_data(int_val)
        v1 = service.to_object(d1)
        v2 = service.to_object(d2)
        v3 = service.to_object(d3)

        self.assertEqual(d1.get_type(), CONSTANT_TYPE_INTEGER)
        self.assertEqual(d2.get_type(), CONSTANT_TYPE_INTEGER)
        self.assertEqual(d3.get_type(), CONSTANT_TYPE_INTEGER)
        self.assertEqual(v1, byte_val)
        self.assertEqual(v2, short_val)
        self.assertEqual(v3, int_val)
        with self.assertRaises(HazelcastSerializationError):
            service.to_data(big_int)

    def test_long_case(self):
        config = SerializationConfig()
        config.default_integer_type = INTEGER_TYPE.LONG
        service = SerializationServiceV1(serialization_config=config)

        d1 = service.to_data(byte_val)
        d2 = service.to_data(short_val)
        d3 = service.to_data(int_val)
        d4 = service.to_data(long_val)
        v1 = service.to_object(d1)
        v2 = service.to_object(d2)
        v3 = service.to_object(d3)
        v4 = service.to_object(d4)

        self.assertEqual(d1.get_type(), CONSTANT_TYPE_LONG)
        self.assertEqual(d2.get_type(), CONSTANT_TYPE_LONG)
        self.assertEqual(d3.get_type(), CONSTANT_TYPE_LONG)
        self.assertEqual(d4.get_type(), CONSTANT_TYPE_LONG)
        self.assertEqual(v1, byte_val)
        self.assertEqual(v2, short_val)
        self.assertEqual(v3, int_val)
        self.assertEqual(v4, long_val)
        with self.assertRaises(HazelcastSerializationError):
            service.to_data(big_int)
