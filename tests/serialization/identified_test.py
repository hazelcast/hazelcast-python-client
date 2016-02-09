import binascii
import struct
import unittest

import hazelcast
from hazelcast.serialization import SerializationServiceV1
from hazelcast.serialization.api import IdentifiedDataSerializable

FACTORY_ID = 1


class SampleIdentified(IdentifiedDataSerializable):
    CLASS_ID = 1

    def __init__(self, a_byte=None, a_boolean=None, a_character=None, a_short=None, a_integer=None, a_long=None, a_float=None,
                 a_double=None, a_string=None):
        self.a_byte = a_byte
        self.a_boolean = a_boolean
        self.a_character = a_character
        self.a_short = a_short
        self.a_integer = a_integer
        self.a_long = a_long
        self.a_float = a_float
        self.a_double = a_double
        self.a_string = a_string

    def write_data(self, out):
        out.write_byte(self.a_byte)
        out.write_boolean(self.a_boolean)
        # out.write_char(self.a_character)
        out.write_short(self.a_short)
        out.write_int(self.a_integer)
        out.write_long(self.a_long)
        out.write_float(self.a_float)
        out.write_double(self.a_double)
        out.write_utf(self.a_string)

    def read_data(self, inp):
        self.a_byte = inp.read_byte()
        self.a_boolean = inp.read_boolean()
        # self.a_character = inp.read_char()
        self.a_short = inp.read_short()
        self.a_integer = inp.read_int()
        self.a_long = inp.read_long()
        self.a_float = inp.read_float()
        self.a_double = inp.read_double()
        self.a_string = inp.read_utf()

    def get_factory_id(self):
        return FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID

    def __eq__(self, other):
        # self.a_character == other.a_character and \
        byte = self.a_byte == other.a_byte
        boolean = self.a_boolean == other.a_boolean
        short = self.a_short == other.a_short
        integer = self.a_integer == other.a_integer
        long = self.a_long == other.a_long
        float = self.a_float == other.a_float
        double = self.a_double == other.a_double
        string = self.a_string == other.a_string
        return byte and boolean and short and integer and long and float and double and string


the_factory = {SampleIdentified.CLASS_ID: SampleIdentified}


class IdentifiedSerializationTestCase(unittest.TestCase):
    def test_factory(self):
        config = hazelcast.ClientConfig()
        config.serialization_config.data_serializable_factories[FACTORY_ID] = the_factory
        service = SerializationServiceV1(config.serialization_config)
        obj = SampleIdentified(1, True, "a", 0x1234, 0x12345678, 0x1234567890123456, 1.0, 2.2, "TEST")
        data = service.to_data(obj)

        obj2 = service.to_object(data)
        self.assertTrue(obj == obj2)
