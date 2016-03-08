import binascii
import struct
import unittest

import hazelcast
from hazelcast.serialization import SerializationServiceV1
from hazelcast.serialization.api import IdentifiedDataSerializable, Portable

FACTORY_ID = 1


class SamplePortable(Portable):
    CLASS_ID = 8

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

    def write_portable(self, out):
        out.write_byte("1", self.a_byte)
        out.write_boolean("2", self.a_boolean)
        # out.write_char("3", self.a_character)
        out.write_short("4", self.a_short)
        out.write_int("5", self.a_integer)
        out.write_long("6", self.a_long)
        out.write_float("7", self.a_float)
        out.write_double("8", self.a_double)
        out.write_utf("9", self.a_string)

    def read_portable(self, inp):
        self.a_byte = inp.read_byte("1")
        self.a_boolean = inp.read_boolean("2")
        # self.a_character = inp.read_char("3")
        self.a_short = inp.read_short("4")
        self.a_integer = inp.read_int("5")
        self.a_long = inp.read_long("6")
        self.a_float = inp.read_float("7")
        self.a_double = inp.read_double("8")
        self.a_string = inp.read_utf("9")

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


the_factory = {SamplePortable.CLASS_ID: SamplePortable}


class PortableSerializationTestCase(unittest.TestCase):
    def test_factory(self):
        config = hazelcast.ClientConfig()
        config.serialization_config.portable_factories[FACTORY_ID] = the_factory
        service = SerializationServiceV1(config.serialization_config)
        obj = SamplePortable(99, True, "c", 11, 1234134, 1341431221, 1.0, 1.0/111, "the string text")
        data = service.to_data(obj)
        obj2 = service.to_object(data)
        self.assertTrue(obj == obj2)
