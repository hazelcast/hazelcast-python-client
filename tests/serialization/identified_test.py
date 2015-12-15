import unittest

from hazelcast.serialization.api import IdentifiedDataSerializable
from hazelcast.serialization import SerializationServiceV1


class _Sample_Identified(IdentifiedDataSerializable):
    def __init__(self, a_byte, a_boolean, a_character, a_short, a_integer, a_long, a_float, a_double, a_string):
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
        out.write_char(self.a_character)
        out.write_short(self.a_short)
        out.write_int(self.a_integer)
        out.write_long(self.a_long)
        out.write_float(self.a_float)
        out.write_double(self.a_double)
        out.write_utf(self.a_string)

    def read_data(self, inp):
        self.a_byte = inp.read_byte()
        self.a_boolean = inp.read_boolean()
        self.a_character = inp.read_char()
        self.a_short = inp.read_short()
        self.a_character = inp.read_char()
        self.a_long = inp.read_long()
        self.a_float = inp.read_float()
        self.a_double = inp.read_double()
        self.a_string = inp.read_utf()

    def get_factory_id(self):
        return 1

    def get_class_id(self):
        return 1


class _Identified_Serialization_Test_Case(unittest._Test_Case):
    def test_factory(self):
        service = SerializationServiceV1()
        obj = "Test obj"
        data = service.to_data(obj)

        obj2 = service.to_object(data)
        self.assert_Equal(obj, obj2)


if __name__ == '__main__':
    unittest.main()
