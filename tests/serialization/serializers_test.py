import unittest

import binascii

from hazelcast.config import SerializationConfig
from hazelcast.serialization.data import Data
from hazelcast.serialization.service import SerializationServiceV1


class SerializersTestCase(unittest.TestCase):
    def setUp(self):
        self.service = SerializationServiceV1(serialization_config=SerializationConfig())

    def tearDown(self):
        self.service.destroy()

    def test_none_serializer(self):
        none = None
        data_n = self.service.to_data(none)
        self.assertIsNone(data_n)
        self.assertIsNone(self.service.to_object(Data()))

    def test_boolean_serializer(self):
        true = True
        false = False
        data_t = self.service.to_data(true)
        data_f = self.service.to_data(false)

        obj_t = self.service.to_object(data_t)
        obj_f = self.service.to_object(data_f)
        self.assertEqual(true, obj_t)
        self.assertEqual(false, obj_f)

    def test_char_type_serializer(self):
        buff = bytearray(binascii.unhexlify("00000000fffffffb00e7"))
        data = Data(buff)
        obj = self.service.to_object(data)
        self.assertEqual(unichr(0x00e7), obj)

    def test_float(self):
        buff = bytearray(binascii.unhexlify("00000000fffffff700000000"))
        data = Data(buff)
        obj = self.service.to_object(data)
        self.assertEqual(0.0, obj)
