import unittest

import hazelcast
from hazelcast.exception import HazelcastSerializationError
from hazelcast.serialization import SerializationServiceV1
from hazelcast.serialization.api import Portable
from hazelcast.serialization.portable.classdef import ClassDefinitionBuilder
from tests.serialization.identified_test import create_identified, SerializationV1Identified

FACTORY_ID = 1


class SerializationV1Portable(Portable):
    CLASS_ID = 8

    def __init__(self, a_byte=0, a_boolean=False, a_character=chr(0), a_short=0, a_integer=0, a_long=0, a_float=0.0,
                 a_double=0.0, bytes_=None, booleans=None, chars=None, shorts=None, ints=None, longs=None,
                 floats=None, doubles=None, string=None, strings=None, inner_portable=None, inner_portable_array=None,
                 identified_serializable=None):
        self.a_byte = a_byte
        self.a_boolean = a_boolean
        self.a_character = a_character
        self.a_short = a_short
        self.a_integer = a_integer
        self.a_long = a_long
        self.a_float = a_float
        self.a_double = a_double
        self.bytes = bytes_
        self.booleans = booleans
        self.chars = chars
        self.shorts = shorts
        self.ints = ints
        self.longs = longs
        self.floats = floats
        self.doubles = doubles
        self.a_string = string
        self.strings = strings
        self.inner_portable = inner_portable
        self.inner_portable_array = inner_portable_array
        self.identified_serializable = identified_serializable
        self.nested_field = None

    def write_portable(self, out):
        out.write_byte("1", self.a_byte)
        out.write_boolean("2", self.a_boolean)
        out.write_char("3", self.a_character)
        out.write_short("4", self.a_short)
        out.write_int("5", self.a_integer)
        out.write_long("6", self.a_long)
        out.write_float("7", self.a_float)
        out.write_double("8", self.a_double)
        out.write_utf("9", self.a_string)

        out.write_byte_array("a1", self.bytes)
        out.write_boolean_array("a2", self.booleans)
        out.write_char_array("a3", self.chars)
        out.write_short_array("a4", self.shorts)
        out.write_int_array("a5", self.ints)
        out.write_long_array("a6", self.longs)
        out.write_float_array("a7", self.floats)
        out.write_double_array("a8", self.doubles)
        out.write_utf_array("a9", self.strings)

        if self.inner_portable is None:
            out.write_null_portable("p", FACTORY_ID, InnerPortable.CLASS_ID)
        else:
            out.write_portable("p", self.inner_portable)

        out.write_portable_array("ap", self.inner_portable_array)

        raw_data_output = out.get_raw_data_output()
        not_none = self.identified_serializable is not None

        raw_data_output.write_boolean(not_none)
        if not_none:
            self.identified_serializable.write_data(raw_data_output)

    def read_portable(self, inp):
        self.a_byte = inp.read_byte("1")
        self.a_boolean = inp.read_boolean("2")
        self.a_character = inp.read_char("3")
        self.a_short = inp.read_short("4")
        self.a_integer = inp.read_int("5")
        self.a_long = inp.read_long("6")
        self.a_float = inp.read_float("7")
        self.a_double = inp.read_double("8")
        self.a_string = inp.read_utf("9")

        self.bytes = inp.read_byte_array("a1")
        self.booleans = inp.read_boolean_array("a2")
        self.chars = inp.read_char_array("a3")
        self.shorts = inp.read_short_array("a4")
        self.ints = inp.read_int_array("a5")
        self.longs = inp.read_long_array("a6")
        self.floats = inp.read_float_array("a7")
        self.doubles = inp.read_double_array("a8")
        self.strings = inp.read_utf_array("a9")

        self.inner_portable = inp.read_portable("p")

        self.inner_portable_array = inp.read_portable_array("ap")

        if self.inner_portable:
            self.nested_field = inp.read_int("p.param_int")

        raw_data_input = inp.get_raw_data_input()
        not_none = raw_data_input.read_boolean()
        if not_none:
            identified = SerializationV1Identified()
            identified.read_data(raw_data_input)
            self.identified_serializable = identified

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
        long_ = self.a_long == other.a_long
        float_ = self.a_float == other.a_float
        double = self.a_double == other.a_double
        bytes_ = self.bytes == other.bytes
        booleans = self.booleans == other.booleans
        chars = self.chars == other.chars
        shorts = self.shorts == other.shorts
        integers = self.ints == other.ints
        longs = self.longs == other.longs
        floats = self.floats == other.floats
        doubles = self.doubles == other.doubles
        string = self.a_string == other.a_string
        strings = self.strings == other.strings
        inner_portable = self.inner_portable == other.inner_portable
        inner_portable_array = self.inner_portable_array == other.inner_portable_array
        identified_serializable = self.identified_serializable == other.identified_serializable
        return byte and boolean and char and short and integer and long_ and float_ and double and \
               bytes_ and booleans and chars and shorts and integers and longs and floats and doubles and \
               string and strings and inner_portable and inner_portable_array and identified_serializable


class InnerPortable(Portable):
    CLASS_ID = 1

    def __init__(self, param_str=None, param_int=None):
        self.param_str = param_str
        self.param_int = param_int

    def write_portable(self, writer):
        writer.write_utf("param_str", self.param_str)
        writer.write_int("param_int", self.param_int)

    def read_portable(self, reader):
        self.param_str = reader.read_utf("param_str")
        self.param_int = reader.read_int("param_int")

    def get_factory_id(self):
        return FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID

    def __eq__(self, other):
        return self.param_str == other.param_str and self.param_int == other.param_int


def create_portable():
    identified = create_identified()
    inner_portable = InnerPortable("Inner Text", 666)
    return SerializationV1Portable(99, True, 'c', 11, 1234134, 1341431221l, 1.0, 2.0, [1, 2, 3], [True, False, True],
                                   ['a', 'b', 'c'],
                                   [1, 2, 3], [4, 2, 3], [11, 2, 3], [1.0, 2.0, 3.0],
                                   [11.0, 22.0, 33.0], "the string text",
                                   ["item1", "item2", "item3"], inner_portable,
                                   [InnerPortable("Portable array item 0", 0), InnerPortable("Portable array item 1", 1)],
                                   identified)


the_factory = {SerializationV1Portable.CLASS_ID: SerializationV1Portable, InnerPortable.CLASS_ID: InnerPortable}


class PortableSerializationTestCase(unittest.TestCase):
    def test_encode_decode(self):
        config = hazelcast.ClientConfig()
        config.serialization_config.portable_factories[FACTORY_ID] = the_factory
        service = SerializationServiceV1(config.serialization_config)
        obj = create_portable()
        self.assertTrue(obj.inner_portable)

        data = service.to_data(obj)
        obj2 = service.to_object(data)
        self.assertTrue(obj == obj2)
        self.assertEquals(obj.inner_portable.param_int, obj2.nested_field)

    def test_encode_decode_2(self):
        config = hazelcast.ClientConfig()
        config.serialization_config.portable_factories[FACTORY_ID] = the_factory
        service = SerializationServiceV1(config.serialization_config)
        service2 = SerializationServiceV1(config.serialization_config)
        obj = create_portable()
        self.assertTrue(obj.inner_portable)

        data = service.to_data(obj)
        obj2 = service2.to_object(data)
        self.assertTrue(obj == obj2)

    def test_portable_context(self):
        config = hazelcast.ClientConfig()
        config.serialization_config.portable_factories[FACTORY_ID] = the_factory
        service = SerializationServiceV1(config.serialization_config)
        obj = create_portable()
        self.assertTrue(obj.inner_portable)

        service.to_data(obj)

        class_definition = service._portable_context.lookup_class_definition(FACTORY_ID, InnerPortable.CLASS_ID, 0)
        self.assertTrue(class_definition is not None)

    def test_portable_null_fields(self):
        config = hazelcast.ClientConfig()
        config.serialization_config.portable_factories[FACTORY_ID] = the_factory
        service = SerializationServiceV1(config.serialization_config)
        service.to_data(create_portable())

        service2 = SerializationServiceV1(config.serialization_config)
        obj = SerializationV1Portable()

        data = service.to_data(obj)
        obj2 = service2.to_object(data)
        self.assertTrue(obj == obj2)
    
    def test_portable_class_def(self):
        builder_inner = ClassDefinitionBuilder(FACTORY_ID, InnerPortable.CLASS_ID)
        builder_inner.add_utf_field("param_str")
        builder_inner.add_int_field("param_int")
        class_def_inner = builder_inner.build()

        builder = ClassDefinitionBuilder(FACTORY_ID, SerializationV1Portable.CLASS_ID)

        builder.add_byte_field("1")
        builder.add_boolean_field("2")
        builder.add_char_field("3")
        builder.add_short_field("4")
        builder.add_int_field("5")
        builder.add_long_field("6")
        builder.add_float_field("7")
        builder.add_double_field("8")
        builder.add_utf_field("9")
        builder.add_byte_array_field("a1")
        builder.add_boolean_array_field("a2")
        builder.add_char_array_field("a3")
        builder.add_short_array_field("a4")
        builder.add_int_array_field("a5")
        builder.add_long_array_field("a6")
        builder.add_float_array_field("a7")
        builder.add_double_array_field("a8")
        builder.add_utf_array_field("a9")
        builder.add_portable_field("p", class_def_inner)
        builder.add_portable_array_field("ap", class_def_inner)
        class_def = builder.build()

        config = hazelcast.ClientConfig()
        config.serialization_config.portable_factories[FACTORY_ID] = the_factory

        config.serialization_config.class_definitions.add(class_def)
        config.serialization_config.class_definitions.add(class_def_inner)

        service = SerializationServiceV1(config.serialization_config)
        service2 = SerializationServiceV1(config.serialization_config)
        obj = SerializationV1Portable()

        data = service.to_data(obj)
        obj2 = service2.to_object(data)
        self.assertTrue(obj == obj2)

    def test_portable_read_without_factory(self):
        config = hazelcast.ClientConfig()
        config.serialization_config.portable_factories[FACTORY_ID] = the_factory
        service = SerializationServiceV1(config.serialization_config)
        service2 = SerializationServiceV1(hazelcast.SerializationConfig())
        obj = create_portable()
        self.assertTrue(obj.inner_portable)

        data = service.to_data(obj)
        with self.assertRaises(HazelcastSerializationError):
            service2.to_object(data)
