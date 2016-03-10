import unittest

import hazelcast
from hazelcast.serialization import SerializationServiceV1
from hazelcast.serialization.api import IdentifiedDataSerializable

FACTORY_ID = 1


class SerializationV1Identified(IdentifiedDataSerializable):
    CLASS_ID = 1

    def __init__(self, a_byte=None, a_boolean=None, a_character=None, a_short=None, a_integer=None, a_long=None, a_float=None,
                 a_double=None, bytes_=None, booleans=None, chars=None, shorts=None, ints=None, longs=None,
                 floats=None, doubles=None, a_string=None, strings=None):
        self.a_byte = a_byte
        self.a_boolean = a_boolean
        self.a_character = a_character
        self.a_short = a_short
        self.a_integer = a_integer
        self.a_long = a_long
        self.a_float = a_float
        self.a_double = a_double
        self.bytes_ = bytes_
        self.booleans = booleans
        self.chars = chars
        self.shorts = shorts
        self.ints = ints
        self.longs = longs
        self.floats = floats
        self.doubles = doubles
        self.a_string = a_string
        self.strings = strings

    def write_data(self, out):
        out.write_byte(self.a_byte)
        out.write_boolean(self.a_boolean)
        out.write_char(self.a_character)
        out.write_short(self.a_short)
        out.write_int(self.a_integer)
        out.write_long(self.a_long)
        out.write_float(self.a_float)
        out.write_double(self.a_double)

        out.write_byte_array(self.bytes_)
        out.write_boolean_array(self.booleans)
        out.write_char_array(self.chars)
        out.write_short_array(self.shorts)
        out.write_int_array(self.ints)
        out.write_long_array(self.longs)
        out.write_float_array(self.floats)
        out.write_double_array(self.doubles)
        out.write_utf(self.a_string)
        out.write_utf_array(self.strings)

    def read_data(self, inp):
        self.a_byte = inp.read_byte()
        self.a_boolean = inp.read_boolean()
        self.a_character = inp.read_char()
        self.a_short = inp.read_short()
        self.a_integer = inp.read_int()
        self.a_long = inp.read_long()
        self.a_float = inp.read_float()
        self.a_double = inp.read_double()

        self.bytes_ = inp.read_byte_array()
        self.booleans = inp.read_boolean_array()
        self.chars = inp.read_char_array()
        self.shorts = inp.read_short_array()
        self.ints = inp.read_int_array()
        self.longs = inp.read_long_array()
        self.floats = inp.read_float_array()
        self.doubles = inp.read_double_array()
        self.a_string = inp.read_utf()
        self.strings = inp.read_utf_array()

    def get_factory_id(self):
        return FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID

    def __eq__(self, other):
        byte = self.a_byte == other.a_byte
        boolean = self.a_boolean == other.a_boolean
        char = self.a_character == other.a_character
        short = self.a_short == other.a_short
        integer = self.a_integer == other.a_integer
        long = self.a_long == other.a_long
        float = self.a_float == other.a_float
        double = self.a_double == other.a_double
        bytes_ = self.bytes_ == other.bytes_
        booleans = self.booleans == other.booleans
        chars = self.chars == other.chars
        shorts = self.shorts == other.shorts
        integers = self.ints == other.ints
        longs = self.longs == other.longs
        floats = self.floats == other.floats
        doubles = self.doubles == other.doubles
        string = self.a_string == other.a_string
        strings = self.strings == other.strings
        return byte and boolean and char and short and integer and long and float and double and \
               bytes_ and booleans and chars and shorts and integers and longs and floats and doubles and string and strings


def create_identified():
    return SerializationV1Identified(99, True, 'c', 11, 1234134, 1341431221, 1.0, 2.0, [1, 2, 3], [True, False, True],
                                     ['a', 'b', 'c'], [1, 2, 3], [4, 2, 3], [11, 2, 3], [1.0, 2.0, 3.0], [11.0, 22.0, 33.0],
                                     "the string text", ["item1", "item2", "item3"])


the_factory = {SerializationV1Identified.CLASS_ID: SerializationV1Identified}


class IdentifiedSerializationTestCase(unittest.TestCase):

    def test_encode_decode(self):
        config = hazelcast.ClientConfig()
        config.serialization_config.data_serializable_factories[FACTORY_ID] = the_factory
        service = SerializationServiceV1(config.serialization_config)
        obj = create_identified()
        data = service.to_data(obj)

        obj2 = service.to_object(data)
        self.assertTrue(obj == obj2)
