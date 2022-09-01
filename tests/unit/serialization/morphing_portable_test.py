import datetime
import decimal
import unittest

from hazelcast.config import _Config
from hazelcast.serialization import SerializationServiceV1
from hazelcast.serialization.data import DATA_OFFSET
from hazelcast.serialization.input import _ObjectDataInput
from tests.unit.serialization.portable_test import (
    create_portable,
    SerializationV1Portable,
    InnerPortable,
    FACTORY_ID,
)


class MorphingPortable(SerializationV1Portable):
    @classmethod
    def clone(cls, base):
        return MorphingPortable(
            base.a_byte,
            base.a_boolean,
            base.a_character,
            base.a_short,
            base.a_integer,
            base.a_long,
            base.a_float,
            base.a_double,
            base.a_decimal,
            base.a_time,
            base.a_date,
            base.a_timestamp,
            base.a_timestamp_with_timezone,
            base.bytes,
            base.booleans,
            base.chars,
            base.shorts,
            base.ints,
            base.longs,
            base.floats,
            base.doubles,
            base.decimals,
            base.times,
            base.dates,
            base.timestamps,
            base.timestamp_with_timezones,
            base.a_string,
            base.strings,
            base.inner_portable,
            base.inner_portable_array,
            base.identified_serializable,
        )

    def get_class_version(self):
        return 2


the_factory_1 = {
    SerializationV1Portable.CLASS_ID: SerializationV1Portable,
    InnerPortable.CLASS_ID: InnerPortable,
}
the_factory_2 = {
    SerializationV1Portable.CLASS_ID: MorphingPortable,
    InnerPortable.CLASS_ID: InnerPortable,
}


class MorphingPortableTestCase(unittest.TestCase):
    def setUp(self):
        config1 = _Config()
        config1.portable_factories = {FACTORY_ID: the_factory_1}

        config2 = _Config()
        config2.portable_factories = {FACTORY_ID: the_factory_2}

        self.service1 = SerializationServiceV1(config1)
        self.service2 = SerializationServiceV1(config2)

        base_portable = create_portable()
        data = self.service1.to_data(base_portable)

        inp = _ObjectDataInput(
            data.buffer, DATA_OFFSET, self.service2, self.service2._is_big_endian
        )
        portable_serializer = self.service2._registry._portable_serializer
        self.reader = portable_serializer.create_morphing_reader(inp)

    def tearDown(self):
        self.service1.destroy()
        self.service2.destroy()

    def test_read_long(self):
        a_byte = self.reader.read_long("1")
        a_character = self.reader.read_long("3")
        a_short = self.reader.read_long("4")
        a_integer = self.reader.read_long("5")
        a_long = self.reader.read_long("6")
        self.assertEqual(99, a_byte)
        self.assertEqual("c", a_character)
        self.assertEqual(11, a_short)
        self.assertEqual(1234134, a_integer)
        self.assertEqual(1341431221, a_long)
        self.assertEqual(0, self.reader.read_long("NO SUCH FIELD"))

    def test_read_int(self):
        a_byte = self.reader.read_int("1")
        a_character = self.reader.read_int("3")
        a_short = self.reader.read_int("4")
        a_integer = self.reader.read_int("5")
        self.assertEqual(99, a_byte)
        self.assertEqual("c", a_character)
        self.assertEqual(11, a_short)
        self.assertEqual(1234134, a_integer)
        self.assertEqual(0, self.reader.read_int("NO SUCH FIELD"))

    def test_read_short(self):
        a_byte = self.reader.read_short("1")
        a_short = self.reader.read_short("4")
        self.assertEqual(99, a_byte)
        self.assertEqual(11, a_short)
        self.assertEqual(0, self.reader.read_short("NO SUCH FIELD"))

    def test_read_float(self):
        a_byte = self.reader.read_float("1")
        a_character = self.reader.read_float("3")
        a_short = self.reader.read_float("4")
        a_integer = self.reader.read_float("5")
        a_float = self.reader.read_float("7")
        self.assertEqual(99, a_byte)
        self.assertEqual("c", a_character)
        self.assertEqual(11, a_short)
        self.assertEqual(1234134, a_integer)
        self.assertEqual(1.0, a_float)
        self.assertEqual(0, self.reader.read_float("NO SUCH FIELD"))

    def test_read_double(self):
        a_byte = self.reader.read_double("1")
        a_character = self.reader.read_double("3")
        a_short = self.reader.read_double("4")
        a_integer = self.reader.read_double("5")
        a_long = self.reader.read_double("6")
        a_float = self.reader.read_double("7")
        a_double = self.reader.read_double("8")
        self.assertEqual(99, a_byte)
        self.assertEqual("c", a_character)
        self.assertEqual(11, a_short)
        self.assertEqual(1234134, a_integer)
        self.assertEqual(1341431221, a_long)
        self.assertEqual(1.0, a_float)
        self.assertEqual(2.0, a_double)
        self.assertEqual(0, self.reader.read_double("NO SUCH FIELD"))

    def test_read_byte(self):
        a_byte = self.reader.read_byte("1")
        self.assertEqual(99, a_byte)
        self.assertEqual(0, self.reader.read_byte("NO SUCH FIELD"))

    def test_read_boolean(self):
        a_boolean = self.reader.read_boolean("2")
        self.assertTrue(a_boolean)
        self.assertFalse(self.reader.read_boolean("NO SUCH FIELD"))

    def test_read_char(self):
        a_character = self.reader.read_char("3")
        self.assertEqual("c", a_character)
        self.assertEqual(0, self.reader.read_char("NO SUCH FIELD"))

    def test_read_decimal(self):
        a_decimal = self.reader.read_decimal("10")
        self.assertEqual(decimal.Decimal(0.00005), a_decimal)
        self.assertFalse(self.reader.read_decimal("NO SUCH FIELD"))

    def test_read_time(self):
        a_time = self.reader.read_time("11")
        self.assertEqual(datetime.time(23, 59, 59), a_time)
        self.assertFalse(self.reader.read_time("NO SUCH FIELD"))

    def test_read_date(self):
        a_date = self.reader.read_date("12")
        self.assertEqual(datetime.date(1923, 4, 23), a_date)
        self.assertFalse(self.reader.read_date("NO SUCH FIELD"))

    def test_read_timestamp(self):
        a_timestamp = self.reader.read_timestamp("13")
        self.assertEqual(datetime.datetime(1938, 11, 10, 9, 5, 59, 59), a_timestamp)
        self.assertFalse(self.reader.read_timestamp("NO SUCH FIELD"))

    def test_read_timestamp_with_timezone(self):
        a_timestamp_with_timezone = self.reader.read_timestamp_with_timezone("14")
        self.assertEqual(
            datetime.datetime(
                1919, 5, 19, 13, 30, 45, 59, datetime.timezone(datetime.timedelta(seconds=12345))
            ),
            a_timestamp_with_timezone,
        )
        self.assertFalse(self.reader.read_timestamp_with_timezone("NO SUCH FIELD"))

    def test_encode_decode_with_parent_default_reader(self):
        obj = MorphingPortable.clone(create_portable())
        self.assertTrue(obj.inner_portable)

        data = self.service2.to_data(obj)
        obj2 = self.service1.to_object(data)
        self.assertTrue(obj == obj2)

    def test_incompatible_types(self):
        functions = [
            self.reader.read_byte,
            self.reader.read_boolean,
            self.reader.read_char,
            self.reader.read_short,
            self.reader.read_int,
            self.reader.read_long,
            self.reader.read_float,
            self.reader.read_double,
            self.reader.read_string_array,
            self.reader.read_short_array,
            self.reader.read_int_array,
            self.reader.read_long_array,
            self.reader.read_float_array,
            self.reader.read_double_array,
            self.reader.read_char_array,
            self.reader.read_byte_array,
            self.reader.read_boolean_array,
            self.reader.read_portable,
            self.reader.read_portable_array,
        ]
        for read_fnc in functions:
            with self.assertRaises(TypeError):
                read_fnc("9")
        with self.assertRaises(TypeError):
            self.reader.read_string("1")

    def test_missing_fields(self):
        functions = [
            self.reader.read_string,
            self.reader.read_string_array,
            self.reader.read_short_array,
            self.reader.read_int_array,
            self.reader.read_long_array,
            self.reader.read_float_array,
            self.reader.read_double_array,
            self.reader.read_char_array,
            self.reader.read_byte_array,
            self.reader.read_boolean_array,
            self.reader.read_portable,
            self.reader.read_portable_array,
        ]
        for read_fnc in functions:
            self.assertIsNone(read_fnc("NO SUCH FIELD"))

    def test_reader_get_version(self):
        self.assertEqual(0, self.reader.get_version())

    def test_reader_has_field(self):
        self.assertTrue(self.reader.has_field("1"))
        self.assertFalse(self.reader.has_field("NO SUCH FIELD"))

    def test_reader_get_field_names(self):
        expected_names = {
            "1",
            "2",
            "3",
            "4",
            "5",
            "6",
            "7",
            "8",
            "9",
            "10",
            "11",
            "12",
            "13",
            "14",
            "a1",
            "a2",
            "a3",
            "a4",
            "a5",
            "a6",
            "a7",
            "a8",
            "a9",
            "a10",
            "a11",
            "a12",
            "a13",
            "a14",
            "p",
            "ap",
        }
        field_names = set(self.reader.get_field_names())
        self.assertSetEqual(expected_names, field_names)

    def test_reader_get_field_type(self):
        self.assertIsNotNone(self.reader.get_field_type("1"))

    def test_reader_get_field_class_id(self):
        self.assertEqual(0, self.reader.get_field_class_id("1"))
