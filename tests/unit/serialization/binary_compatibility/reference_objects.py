# coding=utf-8
import datetime
import decimal
import re
import uuid

from hazelcast import predicate, aggregator, projection
from hazelcast.serialization.api import Portable, IdentifiedDataSerializable
from hazelcast.serialization.data import Data
from hazelcast.util import to_signed

IDENTIFIED_DATA_SERIALIZABLE_FACTORY_ID = 1
PORTABLE_FACTORY_ID = 1
PORTABLE_CLASS_ID = 1
INNER_PORTABLE_CLASS_ID = 2
DATA_SERIALIZABLE_CLASS_ID = 1
CUSTOM_STREAM_SERIALIZABLE_ID = 1
CUSTOM_BYTE_ARRAY_SERIALIZABLE_ID = 2


def _almost_equal(a, b):
    return abs(a - b) <= max(1e-04 * max(abs(a), abs(b)), 0.0)


def is_equal(a, b):
    if type(a) != type(b):
        return False

    if isinstance(a, float):
        return _almost_equal(a, b)

    if isinstance(a, list):
        n = len(a)
        if n != len(b):
            return False

        for i in range(n):
            if not is_equal(a[i], b[i]):
                return False

        return True

    return a == b


def _get_reference_str():
    s = []
    for i in range(0xFFFF):
        if not 0xD800 <= i < (0xDFFF + 1):
            s.append(i)

    s.extend([0] * (0xFFFF - len(s)))
    return "".join(map(chr, s))


class AnInnerPortable(Portable):
    def __init__(self, i=None, f=None):
        self.i = i
        self.f = f

    def write_portable(self, writer):
        writer.write_int("i", self.i)
        writer.write_float("f", self.f)

    def read_portable(self, reader):
        self.i = reader.read_int("i")
        self.f = reader.read_float("f")

    def get_factory_id(self):
        return PORTABLE_FACTORY_ID

    def get_class_id(self):
        return INNER_PORTABLE_CLASS_ID

    def __eq__(self, other):
        return (
            isinstance(other, AnInnerPortable) and self.i == other.i and is_equal(self.f, other.f)
        )

    def __ne__(self, other):
        return not self.__eq__(other)


class CustomStreamSerializable(object):
    def __init__(self, i=None, f=None):
        self.i = i
        self.f = f

    def __eq__(self, other):
        return (
            isinstance(other, CustomStreamSerializable)
            and self.i == other.i
            and is_equal(self.f, other.f)
        )

    def __ne__(self, other):
        return not self.__eq__(other)


class CustomByteArraySerializable(object):
    def __init__(self, i=None, f=None):
        self.i = i
        self.f = f

    def __eq__(self, other):
        return (
            isinstance(other, CustomByteArraySerializable)
            and self.i == other.i
            and is_equal(self.f, other.f)
        )

    def __ne__(self, other):
        return not self.__eq__(other)


def _write_data_to_out(data, out):
    payload = data.to_bytes() if data is not None else None
    out.write_byte_array(payload)


def _read_data_from_inp(inp):
    buff = inp.read_byte_array()
    return Data(buff) if buff is not None else None


class AnIdentifiedDataSerializable(IdentifiedDataSerializable):
    def __init__(
        self,
        boolean=None,
        b=None,
        c=None,
        d=None,
        s=None,
        f=None,
        i=None,
        l=None,
        string=None,
        booleans=None,
        bytes_=None,
        chars=None,
        doubles=None,
        shorts=None,
        floats=None,
        ints=None,
        longs=None,
        strings=None,
        portable=None,
        identified=None,
        custom_serializable=None,
        custom_byte_array_serializable=None,
        data=None,
    ):
        self.boolean = boolean
        self.b = b
        self.c = c
        self.d = d
        self.s = s
        self.f = f
        self.i = i
        self.l = l
        self.string = string

        self.booleans = booleans
        self.bytes_ = bytes_
        self.chars = chars
        self.doubles = doubles
        self.shorts = shorts
        self.floats = floats
        self.ints = ints
        self.longs = longs
        self.strings = strings

        self.booleans_none = None
        self.bytes_none = None
        self.chars_none = None
        self.doubles_none = None
        self.shorts_none = None
        self.floats_none = None
        self.ints_none = None
        self.longs_none = None
        self.strings_none = None

        self.bytes_size = len(bytes_) if bytes_ is not None else None
        self.bytes_fully = bytes_
        self.bytes_offset = bytes_[1:3] if bytes_ is not None else None
        self.str_chars = list(string) if bytes_ is not None else None
        self.str_bytes = bytearray(map(ord, string)) if bytes_ is not None else None
        self.unsigned_byte = 227
        self.unsigned_short = -32669

        self.portable = portable
        self.identified = identified
        self.custom_serializable = custom_serializable
        self.custom_byte_array_serializable = custom_byte_array_serializable
        self.data = data

    def write_data(self, out):
        out.write_boolean(self.boolean)
        out.write_byte(self.b)
        out.write_char(self.c)
        out.write_double(self.d)
        out.write_short(self.s)
        out.write_float(self.f)
        out.write_int(self.i)
        out.write_long(self.l)
        out.write_string(self.string)

        out.write_boolean_array(self.booleans)
        out.write_byte_array(self.bytes_)
        out.write_char_array(self.chars)
        out.write_double_array(self.doubles)
        out.write_short_array(self.shorts)
        out.write_float_array(self.floats)
        out.write_int_array(self.ints)
        out.write_long_array(self.longs)
        out.write_string_array(self.strings)

        out.write_boolean_array(self.booleans_none)
        out.write_byte_array(self.bytes_none)
        out.write_char_array(self.chars_none)
        out.write_double_array(self.doubles_none)
        out.write_short_array(self.shorts_none)
        out.write_float_array(self.floats_none)
        out.write_int_array(self.ints_none)
        out.write_long_array(self.longs_none)
        out.write_string_array(self.strings_none)

        self.bytes_size = len(self.bytes_)
        out.write_byte(self.bytes_size)
        out.write_from(self.bytes_)
        out.write_byte(self.bytes_[1])
        out.write_byte(self.bytes_[2])
        out.write_int(len(self.string))
        out.write_chars(self.string)
        out.write_bytes(self.string)
        out.write_byte(self.unsigned_byte)
        out.write_short(self.unsigned_short)

        out.write_object(self.portable)
        out.write_object(self.identified)
        out.write_object(self.custom_byte_array_serializable)
        out.write_object(self.custom_serializable)

        _write_data_to_out(self.data, out)

    def read_data(self, inp):
        self.boolean = inp.read_boolean()
        self.b = inp.read_byte()
        self.c = inp.read_char()
        self.d = inp.read_double()
        self.s = inp.read_short()
        self.f = inp.read_float()
        self.i = inp.read_int()
        self.l = inp.read_long()
        self.string = inp.read_string()

        self.booleans = inp.read_boolean_array()
        self.bytes_ = inp.read_byte_array()
        self.chars = inp.read_char_array()
        self.doubles = inp.read_double_array()
        self.shorts = inp.read_short_array()
        self.floats = inp.read_float_array()
        self.ints = inp.read_int_array()
        self.longs = inp.read_long_array()
        self.strings = inp.read_string_array()

        self.booleans_none = inp.read_boolean_array()
        self.bytes_none = inp.read_byte_array()
        self.chars_none = inp.read_char_array()
        self.doubles_none = inp.read_double_array()
        self.shorts_none = inp.read_short_array()
        self.floats_none = inp.read_float_array()
        self.ints_none = inp.read_int_array()
        self.longs_none = inp.read_long_array()
        self.strings_none = inp.read_string_array()

        self.bytes_size = inp.read_byte()
        self.bytes_fully = bytearray(self.bytes_size)
        inp.read_into(self.bytes_fully)
        self.bytes_offset = bytearray(2)
        inp.read_into(self.bytes_offset, 0, 2)
        str_size = inp.read_int()
        self.str_chars = []
        for _ in range(str_size):
            self.str_chars.append(inp.read_char())
        self.str_bytes = bytearray(str_size)
        inp.read_into(self.str_bytes)
        self.unsigned_byte = inp.read_unsigned_byte()
        self.unsigned_short = to_signed(inp.read_unsigned_short(), 16)

        self.portable = inp.read_object()
        self.identified = inp.read_object()
        self.custom_byte_array_serializable = inp.read_object()
        self.custom_serializable = inp.read_object()

        self.data = _read_data_from_inp(inp)

    def get_factory_id(self):
        return IDENTIFIED_DATA_SERIALIZABLE_FACTORY_ID

    def get_class_id(self):
        return DATA_SERIALIZABLE_CLASS_ID

    def __eq__(self, other):
        return (
            isinstance(other, AnIdentifiedDataSerializable)
            and self.boolean == other.boolean
            and self.b == other.b
            and self.c == other.c
            and self.d == other.d
            and self.s == other.s
            and is_equal(self.f, other.f)
            and self.i == other.i
            and self.l == other.l
            and self.bytes_size == other.bytes_size
            and self.unsigned_byte == other.unsigned_byte
            and self.unsigned_short == other.unsigned_short
            and self.string == other.string
            and self.booleans == other.booleans
            and self.bytes_ == other.bytes_
            and self.chars == other.chars
            and self.doubles == other.doubles
            and self.shorts == other.shorts
            and is_equal(self.floats, other.floats)
            and self.ints == other.ints
            and self.longs == other.longs
            and self.strings == other.strings
            and self.booleans_none == other.booleans_none
            and self.bytes_none == other.bytes_none
            and self.chars_none == other.chars_none
            and self.doubles_none == other.doubles_none
            and self.shorts_none == other.shorts_none
            and self.floats_none == other.floats_none
            and self.ints_none == other.ints_none
            and self.longs_none == other.longs_none
            and self.strings_none == other.strings_none
            and self.bytes_fully == other.bytes_fully
            and self.bytes_offset == other.bytes_offset
            and self.str_chars == other.str_chars
            and self.str_bytes == other.str_bytes
            and self.portable == other.portable
            and self.identified == other.identified
            and self.custom_serializable == other.custom_serializable
            and self.custom_byte_array_serializable == other.custom_byte_array_serializable
            and self.data == other.data
        )


class APortable(Portable):
    def __init__(
        self,
        boolean=None,
        b=None,
        c=None,
        d=None,
        s=None,
        f=None,
        i=None,
        l=None,
        string=None,
        p=None,
        booleans=None,
        bytes_=None,
        chars=None,
        doubles=None,
        shorts=None,
        floats=None,
        ints=None,
        longs=None,
        strings=None,
        portables=None,
        identified=None,
        custom_serializable=None,
        custom_byte_array_serializable=None,
        data=None,
    ):
        self.boolean = boolean
        self.b = b
        self.c = c
        self.d = d
        self.s = s
        self.f = f
        self.i = i
        self.l = l
        self.string = string
        self.p = p

        self.booleans = booleans
        self.bytes_ = bytes_
        self.chars = chars
        self.doubles = doubles
        self.shorts = shorts
        self.floats = floats
        self.ints = ints
        self.longs = longs
        self.strings = strings
        self.portables = portables

        self.booleans_none = None
        self.bytes_none = None
        self.chars_none = None
        self.doubles_none = None
        self.shorts_none = None
        self.floats_none = None
        self.ints_none = None
        self.longs_none = None
        self.strings_none = None

        self.bytes_size = len(bytes_) if bytes_ is not None else None
        self.bytes_fully = bytes_
        self.bytes_offset = bytes_[1:3] if bytes_ is not None else None
        self.str_chars = list(string) if bytes_ is not None else None
        self.str_bytes = bytearray(map(ord, string)) if bytes_ is not None else None
        self.unsigned_byte = 227
        self.unsigned_short = -32669

        self.identified = identified
        self.custom_serializable = custom_serializable
        self.custom_byte_array_serializable = custom_byte_array_serializable
        self.data = data

    def write_portable(self, writer):
        writer.write_boolean("bool", self.boolean)
        writer.write_byte("b", self.b)
        writer.write_char("c", self.c)
        writer.write_double("d", self.d)
        writer.write_short("s", self.s)
        writer.write_float("f", self.f)
        writer.write_int("i", self.i)
        writer.write_long("l", self.l)
        writer.write_string("str", self.string)
        if self.p:
            writer.write_portable("p", self.p)
        else:
            writer.write_null_portable("p", PORTABLE_FACTORY_ID, PORTABLE_CLASS_ID)

        writer.write_boolean_array("booleans", self.booleans)
        writer.write_byte_array("bs", self.bytes_)
        writer.write_char_array("cs", self.chars)
        writer.write_double_array("ds", self.doubles)
        writer.write_short_array("ss", self.shorts)
        writer.write_float_array("fs", self.floats)
        writer.write_int_array("is", self.ints)
        writer.write_long_array("ls", self.longs)
        writer.write_string_array("strs", self.strings)
        writer.write_portable_array("ps", self.portables)

        writer.write_boolean_array("booleansNull", self.booleans_none)
        writer.write_byte_array("bsNull", self.bytes_none)
        writer.write_char_array("csNull", self.chars_none)
        writer.write_double_array("dsNull", self.doubles_none)
        writer.write_short_array("ssNull", self.shorts_none)
        writer.write_float_array("fsNull", self.floats_none)
        writer.write_int_array("isNull", self.ints_none)
        writer.write_long_array("lsNull", self.longs_none)
        writer.write_string_array("strsNull", self.strings_none)

        out = writer.get_raw_data_output()

        out.write_boolean(self.boolean)
        out.write_byte(self.b)
        out.write_char(self.c)
        out.write_double(self.d)
        out.write_short(self.s)
        out.write_float(self.f)
        out.write_int(self.i)
        out.write_long(self.l)
        out.write_string(self.string)

        out.write_boolean_array(self.booleans)
        out.write_byte_array(self.bytes_)
        out.write_char_array(self.chars)
        out.write_double_array(self.doubles)
        out.write_short_array(self.shorts)
        out.write_float_array(self.floats)
        out.write_int_array(self.ints)
        out.write_long_array(self.longs)
        out.write_string_array(self.strings)

        out.write_boolean_array(self.booleans_none)
        out.write_byte_array(self.bytes_none)
        out.write_char_array(self.chars_none)
        out.write_double_array(self.doubles_none)
        out.write_short_array(self.shorts_none)
        out.write_float_array(self.floats_none)
        out.write_int_array(self.ints_none)
        out.write_long_array(self.longs_none)
        out.write_string_array(self.strings_none)

        self.bytes_size = len(self.bytes_)
        out.write_byte(self.bytes_size)
        out.write_from(self.bytes_)
        out.write_byte(self.bytes_[1])
        out.write_byte(self.bytes_[2])
        out.write_int(len(self.string))
        out.write_chars(self.string)
        out.write_bytes(self.string)
        out.write_byte(self.unsigned_byte)
        out.write_short(self.unsigned_short)

        out.write_object(self.p)
        out.write_object(self.identified)
        out.write_object(self.custom_byte_array_serializable)
        out.write_object(self.custom_serializable)

        _write_data_to_out(self.data, out)

    def read_portable(self, reader):
        self.boolean = reader.read_boolean("bool")
        self.b = reader.read_byte("b")
        self.c = reader.read_char("c")
        self.d = reader.read_double("d")
        self.s = reader.read_short("s")
        self.f = reader.read_float("f")
        self.i = reader.read_int("i")
        self.l = reader.read_long("l")
        self.string = reader.read_string("str")
        self.p = reader.read_portable("p")

        self.booleans = reader.read_boolean_array("booleans")
        self.bytes_ = reader.read_byte_array("bs")
        self.chars = reader.read_char_array("cs")
        self.doubles = reader.read_double_array("ds")
        self.shorts = reader.read_short_array("ss")
        self.floats = reader.read_float_array("fs")
        self.ints = reader.read_int_array("is")
        self.longs = reader.read_long_array("ls")
        self.strings = reader.read_string_array("strs")
        self.portables = reader.read_portable_array("ps")

        self.booleans_none = reader.read_boolean_array("booleansNull")
        self.bytes_none = reader.read_byte_array("bsNull")
        self.chars_none = reader.read_char_array("csNull")
        self.doubles_none = reader.read_double_array("dsNull")
        self.shorts_none = reader.read_short_array("ssNull")
        self.floats_none = reader.read_float_array("fsNull")
        self.ints_none = reader.read_int_array("isNull")
        self.longs_none = reader.read_long_array("lsNull")
        self.strings_none = reader.read_string_array("strsNull")

        inp = reader.get_raw_data_input()

        self.boolean = inp.read_boolean()
        self.b = inp.read_byte()
        self.c = inp.read_char()
        self.d = inp.read_double()
        self.s = inp.read_short()
        self.f = inp.read_float()
        self.i = inp.read_int()
        self.l = inp.read_long()
        self.string = inp.read_string()

        self.booleans = inp.read_boolean_array()
        self.bytes_ = inp.read_byte_array()
        self.chars = inp.read_char_array()
        self.doubles = inp.read_double_array()
        self.shorts = inp.read_short_array()
        self.floats = inp.read_float_array()
        self.ints = inp.read_int_array()
        self.longs = inp.read_long_array()
        self.strings = inp.read_string_array()

        self.booleans_none = inp.read_boolean_array()
        self.bytes_none = inp.read_byte_array()
        self.chars_none = inp.read_char_array()
        self.doubles_none = inp.read_double_array()
        self.shorts_none = inp.read_short_array()
        self.floats_none = inp.read_float_array()
        self.ints_none = inp.read_int_array()
        self.longs_none = inp.read_long_array()
        self.strings_none = inp.read_string_array()

        self.bytes_size = inp.read_byte()
        self.bytes_fully = bytearray(self.bytes_size)
        inp.read_into(self.bytes_fully)
        self.bytes_offset = bytearray(2)
        inp.read_into(self.bytes_offset, 0, 2)
        str_size = inp.read_int()
        self.str_chars = []
        for _ in range(str_size):
            self.str_chars.append(inp.read_char())
        self.str_bytes = bytearray(str_size)
        inp.read_into(self.str_bytes)
        self.unsigned_byte = inp.read_unsigned_byte()
        self.unsigned_short = to_signed(inp.read_unsigned_short(), 16)

        self.p = inp.read_object()
        self.identified = inp.read_object()
        self.custom_byte_array_serializable = inp.read_object()
        self.custom_serializable = inp.read_object()

        self.data = _read_data_from_inp(inp)

    def get_factory_id(self):
        return PORTABLE_FACTORY_ID

    def get_class_id(self):
        return PORTABLE_CLASS_ID

    def __eq__(self, other):
        return (
            isinstance(other, APortable)
            and self.boolean == other.boolean
            and self.b == other.b
            and self.c == other.c
            and self.d == other.d
            and self.s == other.s
            and is_equal(self.f, other.f)
            and self.i == other.i
            and self.l == other.l
            and self.bytes_size == other.bytes_size
            and self.unsigned_byte == other.unsigned_byte
            and self.unsigned_short == other.unsigned_short
            and self.string == other.string
            and self.p == other.p
            and self.booleans == other.booleans
            and self.bytes_ == other.bytes_
            and self.chars == other.chars
            and self.doubles == other.doubles
            and self.shorts == other.shorts
            and is_equal(self.floats, other.floats)
            and self.ints == other.ints
            and self.longs == other.longs
            and self.strings == other.strings
            and self.portables == other.portables
            and self.booleans_none == other.booleans_none
            and self.bytes_none == other.bytes_none
            and self.chars_none == other.chars_none
            and self.doubles_none == other.doubles_none
            and self.shorts_none == other.shorts_none
            and self.floats_none == other.floats_none
            and self.ints_none == other.ints_none
            and self.longs_none == other.longs_none
            and self.strings_none == other.strings_none
            and self.bytes_fully == other.bytes_fully
            and self.bytes_offset == other.bytes_offset
            and self.str_chars == other.str_chars
            and self.str_bytes == other.str_bytes
            and self.identified == other.identified
            and self.custom_serializable == other.custom_serializable
            and self.custom_byte_array_serializable == other.custom_byte_array_serializable
            and self.data == other.data
        )


_sql_string = "this > 5 AND this < 100"

REFERENCE_OBJECTS = {
    "NULL": None,
    "Boolean": True,
    "Byte": 113,
    "Character": "x",
    "Double": -897543.3678909,
    "Short": -500,
    "Float": 900.5678,
    "Integer": 56789,
    "Long": -50992225,
    "String": _get_reference_str(),
    "UUID": uuid.UUID(hex="ffffffff-fcf5-eb9f-0000-00000000ddd5"),
    "boolean[]": [True, False, True],
    "byte[]": bytearray([112, 4, 255, 4, 112, 221, 43]),
    "char[]": ["a", "b", "c"],
    "double[]": [-897543.3678909, 11.1, 22.2, 33.3],
    "short[]": [-500, 2, 3],
    "float[]": [900.5678, 1.0, 2.1, 3.4],
    "int[]": [56789, 2, 3],
    "long[]": [-50992225, 1231232141, 2, 3],
    "String[]": [
        "Pijamalı hasta, yağız şoföre çabucak güvendi.",
        "イロハニホヘト チリヌルヲ ワカヨタレソ ツネナラム",
        "The quick brown fox jumps over the lazy dog",
    ],
    "LocalDate": datetime.date(2021, 6, 28),
    "LocalTime": datetime.time(11, 22, 41, 123456),
    "LocalDateTime": datetime.datetime(2021, 6, 28, 11, 22, 41, 123456),
    "OffsetDateTime": datetime.datetime(
        2021, 6, 28, 11, 22, 41, 123456, datetime.timezone(datetime.timedelta(hours=18))
    ),
    "BigInteger": 1314432323232411,
    "BigDecimal": decimal.Decimal("31231.12331"),
    "Class": "java.math.BigDecimal",
}

_data = Data(bytearray([49, 49, 49, 51, 49, 51, 49, 50, 51, 49, 51, 49, 51, 49, 51, 49, 51, 49]))

_inner_portable = AnInnerPortable(REFERENCE_OBJECTS["Integer"], REFERENCE_OBJECTS["Float"])

_custom_serializable = CustomStreamSerializable(
    REFERENCE_OBJECTS["Integer"], REFERENCE_OBJECTS["Float"]
)

_custom_byte_array_serializable = CustomByteArraySerializable(
    REFERENCE_OBJECTS["Integer"], REFERENCE_OBJECTS["Float"]
)

_identified = AnIdentifiedDataSerializable(
    REFERENCE_OBJECTS["Boolean"],
    REFERENCE_OBJECTS["Byte"],
    REFERENCE_OBJECTS["Character"],
    REFERENCE_OBJECTS["Double"],
    REFERENCE_OBJECTS["Short"],
    REFERENCE_OBJECTS["Float"],
    REFERENCE_OBJECTS["Integer"],
    REFERENCE_OBJECTS["Long"],
    _sql_string,
    REFERENCE_OBJECTS["boolean[]"],
    REFERENCE_OBJECTS["byte[]"],
    REFERENCE_OBJECTS["char[]"],
    REFERENCE_OBJECTS["double[]"],
    REFERENCE_OBJECTS["short[]"],
    REFERENCE_OBJECTS["float[]"],
    REFERENCE_OBJECTS["int[]"],
    REFERENCE_OBJECTS["long[]"],
    REFERENCE_OBJECTS["String[]"],
    _inner_portable,
    None,
    _custom_serializable,
    _custom_byte_array_serializable,
    _data,
)

_portables = [_inner_portable, _inner_portable, _inner_portable]

_portable = APortable(
    REFERENCE_OBJECTS["Boolean"],
    REFERENCE_OBJECTS["Byte"],
    REFERENCE_OBJECTS["Character"],
    REFERENCE_OBJECTS["Double"],
    REFERENCE_OBJECTS["Short"],
    REFERENCE_OBJECTS["Float"],
    REFERENCE_OBJECTS["Integer"],
    REFERENCE_OBJECTS["Long"],
    _sql_string,
    _inner_portable,
    REFERENCE_OBJECTS["boolean[]"],
    REFERENCE_OBJECTS["byte[]"],
    REFERENCE_OBJECTS["char[]"],
    REFERENCE_OBJECTS["double[]"],
    REFERENCE_OBJECTS["short[]"],
    REFERENCE_OBJECTS["float[]"],
    REFERENCE_OBJECTS["int[]"],
    REFERENCE_OBJECTS["long[]"],
    REFERENCE_OBJECTS["String[]"],
    _portables,
    _identified,
    _custom_serializable,
    _custom_byte_array_serializable,
    _data,
)

_non_null_list = [
    REFERENCE_OBJECTS["Boolean"],
    REFERENCE_OBJECTS["Double"],
    REFERENCE_OBJECTS["Integer"],
    _sql_string,
    _inner_portable,
    REFERENCE_OBJECTS["byte[]"],
    _custom_serializable,
    _custom_byte_array_serializable,
    _identified,
    _portable,
    REFERENCE_OBJECTS["BigDecimal"],
    REFERENCE_OBJECTS["LocalDate"],
    REFERENCE_OBJECTS["LocalTime"],
    REFERENCE_OBJECTS["OffsetDateTime"],
]

REFERENCE_OBJECTS.update(
    {
        "AnInnerPortable": _inner_portable,
        "CustomStreamSerializable": _custom_serializable,
        "CustomByteArraySerializable": _custom_byte_array_serializable,
        "AnIdentifiedDataSerializable": _identified,
        "APortable": _portable,
        "ArrayList": [None, _non_null_list],
        "LinkedList": [None, _non_null_list],
        "TruePredicate": predicate.true(),
        "FalsePredicate": predicate.false(),
        "SqlPredicate": predicate.sql(_sql_string),
        "EqualPredicate": predicate.equal(_sql_string, REFERENCE_OBJECTS["Integer"]),
        "NotEqualPredicate": predicate.not_equal(_sql_string, REFERENCE_OBJECTS["Integer"]),
        "GreaterLessPredicate": predicate.greater(_sql_string, REFERENCE_OBJECTS["Integer"]),
        "BetweenPredicate": predicate.between(
            _sql_string, REFERENCE_OBJECTS["Integer"], REFERENCE_OBJECTS["Integer"]
        ),
        "LikePredicate": predicate.like(_sql_string, _sql_string),
        "ILikePredicate": predicate.ilike(_sql_string, _sql_string),
        "InPredicate": predicate.in_(
            _sql_string, REFERENCE_OBJECTS["Integer"], REFERENCE_OBJECTS["Integer"]
        ),
        "RegexPredicate": predicate.regex(_sql_string, _sql_string),
        "AndPredicate": predicate.and_(
            predicate.sql(_sql_string),
            predicate.equal(_sql_string, REFERENCE_OBJECTS["Integer"]),
            predicate.not_equal(_sql_string, REFERENCE_OBJECTS["Integer"]),
            predicate.greater(_sql_string, REFERENCE_OBJECTS["Integer"]),
            predicate.greater_or_equal(_sql_string, REFERENCE_OBJECTS["Integer"]),
        ),
        "OrPredicate": predicate.or_(
            predicate.sql(_sql_string),
            predicate.equal(_sql_string, REFERENCE_OBJECTS["Integer"]),
            predicate.not_equal(_sql_string, REFERENCE_OBJECTS["Integer"]),
            predicate.greater(_sql_string, REFERENCE_OBJECTS["Integer"]),
            predicate.greater_or_equal(_sql_string, REFERENCE_OBJECTS["Integer"]),
        ),
        "InstanceOfPredicate": predicate.instance_of(
            "com.hazelcast.nio.serialization.compatibility.CustomStreamSerializable"
        ),
        "DistinctValuesAggregator": aggregator.distinct(_sql_string),
        "MaxAggregator": aggregator.max_(_sql_string),
        "MaxByAggregator": aggregator.max_by(_sql_string),
        "MinAggregator": aggregator.min_(_sql_string),
        "MinByAggregator": aggregator.min_by(_sql_string),
        "CountAggregator": aggregator.count(_sql_string),
        "NumberAverageAggregator": aggregator.number_avg(_sql_string),
        "IntegerAverageAggregator": aggregator.int_avg(_sql_string),
        "LongAverageAggregator": aggregator.long_avg(_sql_string),
        "DoubleAverageAggregator": aggregator.double_avg(_sql_string),
        "IntegerSumAggregator": aggregator.int_sum(_sql_string),
        "LongSumAggregator": aggregator.long_sum(_sql_string),
        "DoubleSumAggregator": aggregator.double_sum(_sql_string),
        "FixedSumAggregator": aggregator.fixed_point_sum(_sql_string),
        "FloatingPointSumAggregator": aggregator.floating_point_sum(_sql_string),
        "SingleAttributeProjection": projection.single_attribute(_sql_string),
        "MultiAttributeProjection": projection.multi_attribute(
            _sql_string, _sql_string, _sql_string
        ),
        "IdentityProjection": projection.identity(),
    }
)

_SKIP_ON_SERIALIZE = {
    "Character",
    "Float",
    "boolean[]",
    "char[]",
    "double[]",
    "short[]",
    "float[]",
    "int[]",
    "long[]",
    "String[]",
    "Class",
    "LocalDateTime",
    "LinkedList",
}


def skip_on_serialize(object_type):
    return object_type in _SKIP_ON_SERIALIZE


_SKIP_ON_DESERIALIZE_PATTERN = re.compile(r"^.*(Predicate|Aggregator|Projection)$")


def skip_on_deserialize(object_type):
    return _SKIP_ON_DESERIALIZE_PATTERN.match(object_type) is not None
