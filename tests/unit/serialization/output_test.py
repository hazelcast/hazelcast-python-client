import unittest

from hazelcast.serialization.output import _ObjectDataOutput


class OutputTestCase(unittest.TestCase):
    def test_write_from(self):
        out = _ObjectDataOutput(10, None)
        b = bytearray(range(10))
        out.write_from(b, 5, 3)
        self.assertEqual(
            b[5 : 5 + 3],
            out.to_byte_array(),
        )

    def test_write_zero_bytes(self):
        out = _ObjectDataOutput(0, None)
        out.write_zero_bytes(10)
        self.assertEqual(
            bytearray(10),
            out.to_byte_array(),
        )
        self.assertEqual(10, out.position())

    def test_write_boolean(self):
        out = _ObjectDataOutput(0, None)
        out.write_boolean(True)
        out.write_boolean(False)
        self.assertEqual(
            bytearray([1, 0]),
            out.to_byte_array(),
        )
        self.assertEqual(2, out.position())

    def test_write_boolean_bit_positional(self):
        out = _ObjectDataOutput(100, None)
        out.write_zero_bytes(1)
        out.write_boolean_bit_positional(True, 0, 0)
        out.write_boolean_bit_positional(False, 0, 1)
        out.write_boolean_bit_positional(True, 0, 2)
        self.assertEqual(
            bytearray([0b101]),
            out.to_byte_array(),
        )
        self.assertEqual(1, out.position())

    def test_write_byte(self):
        out = _ObjectDataOutput(10, None)
        out.write_byte(0)
        out.write_byte(255)
        out.write_byte(127)
        self.assertEqual(
            bytearray([0, 255, 127]),
            out.to_byte_array(),
        )
        self.assertEqual(3, out.position())

    def test_write_signed_byte(self):
        out = _ObjectDataOutput(10, None)
        out.write_signed_byte(0)
        out.write_signed_byte(127)
        out.write_signed_byte(-128)
        self.assertEqual(
            bytearray([0, 127, 128]),
            out.to_byte_array(),
        )
        self.assertEqual(3, out.position())

    def test_write_signed_byte_positional(self):
        out = _ObjectDataOutput(10, None)
        out.write_zero_bytes(3)
        out.write_signed_byte_positional(0, 0)
        out.write_signed_byte_positional(127, 1)
        out.write_signed_byte_positional(-128, 2)
        self.assertEqual(
            bytearray([0, 127, 128]),
            out.to_byte_array(),
        )
        self.assertEqual(3, out.position())

    def test_write_short(self):
        out = _ObjectDataOutput(10, None)
        out.write_short(23456)
        out.write_short(-23456)
        self.assertEqual(
            bytearray([91, 160, 164, 96]),
            out.to_byte_array(),
        )
        self.assertEqual(4, out.position())

    def test_write_short_positional(self):
        out = _ObjectDataOutput(10, None)
        out.write_zero_bytes(4)
        out.write_short_positional(23456, 0)
        out.write_short_positional(-23456, 2)
        self.assertEqual(
            bytearray([91, 160, 164, 96]),
            out.to_byte_array(),
        )
        self.assertEqual(4, out.position())

    def test_write_unsigned_short(self):
        out = _ObjectDataOutput(10, None)
        out.write_unsigned_short(23456)
        out.write_unsigned_short(43456)
        self.assertEqual(
            bytearray([91, 160, 169, 192]),
            out.to_byte_array(),
        )
        self.assertEqual(4, out.position())

    def test_write_char(self):
        out = _ObjectDataOutput(0, None)
        out.write_char("a")
        out.write_char("1")
        self.assertEqual(
            bytearray([0, 97, 0, 49]),
            out.to_byte_array(),
        )
        self.assertEqual(4, out.position())

    def test_write_int(self):
        out = _ObjectDataOutput(8, None)
        out.write_int(42)
        out.write_int(-13)
        self.assertEqual(
            bytearray([0, 0, 0, 42, 255, 255, 255, 243]),
            out.to_byte_array(),
        )
        self.assertEqual(8, out.position())

    def test_write_int_big_endian(self):
        out = _ObjectDataOutput(8, None)
        out.write_int_big_endian(42)
        out.write_int_big_endian(-13)
        self.assertEqual(
            bytearray([0, 0, 0, 42, 255, 255, 255, 243]),
            out.to_byte_array(),
        )
        self.assertEqual(8, out.position())

    def test_write_int_positional(self):
        out = _ObjectDataOutput(8, None)
        out.write_zero_bytes(8)
        out.write_int_positional(42, 0)
        out.write_int_positional(-13, 4)
        self.assertEqual(
            bytearray([0, 0, 0, 42, 255, 255, 255, 243]),
            out.to_byte_array(),
        )
        self.assertEqual(8, out.position())

    def test_write_long(self):
        out = _ObjectDataOutput(10, None)
        out.write_long(1)
        out.write_long(-9223372036854775808)
        self.assertEqual(
            bytearray([0, 0, 0, 0, 0, 0, 0, 1, 128, 0, 0, 0, 0, 0, 0, 0]),
            out.to_byte_array(),
        )
        self.assertEqual(16, out.position())

    def test_write_long_positional(self):
        out = _ObjectDataOutput(10, None)
        out.write_zero_bytes(16)
        out.write_long_positional(1, 0)
        out.write_long_positional(-9223372036854775808, 8)
        self.assertEqual(
            bytearray([0, 0, 0, 0, 0, 0, 0, 1, 128, 0, 0, 0, 0, 0, 0, 0]),
            out.to_byte_array(),
        )
        self.assertEqual(16, out.position())

    def test_write_float(self):
        out = _ObjectDataOutput(10, None)
        out.write_float(42.5)
        out.write_float(-442.4)
        self.assertEqual(
            bytearray([66, 42, 0, 0, 195, 221, 51, 51]),
            out.to_byte_array(),
        )
        self.assertEqual(8, out.position())

    def test_write_float_positional(self):
        out = _ObjectDataOutput(10, None)
        out.write_zero_bytes(8)
        out.write_float_positional(42.5, 0)
        out.write_float_positional(-442.4, 4)
        self.assertEqual(
            bytearray([66, 42, 0, 0, 195, 221, 51, 51]),
            out.to_byte_array(),
        )
        self.assertEqual(8, out.position())

    def test_write_double(self):
        out = _ObjectDataOutput(10, None)
        out.write_double(12345.6789)
        out.write_double(-98765.4321)
        self.assertEqual(
            bytearray([64, 200, 28, 214, 230, 49, 248, 161, 192, 248, 28, 214, 233, 225, 176, 138]),
            out.to_byte_array(),
        )
        self.assertEqual(16, out.position())

    def test_write_double_positional(self):
        out = _ObjectDataOutput(10, None)
        out.write_zero_bytes(16)
        out.write_double_positional(12345.6789, 0)
        out.write_double_positional(-98765.4321, 8)
        self.assertEqual(
            bytearray([64, 200, 28, 214, 230, 49, 248, 161, 192, 248, 28, 214, 233, 225, 176, 138]),
            out.to_byte_array(),
        )
        self.assertEqual(16, out.position())

    def test_write_string(self):
        out = _ObjectDataOutput(10, None)
        out.write_string("a")
        out.write_string(None)
        out.write_string("👹")
        self.assertEqual(
            bytearray([0, 0, 0, 1, 97, 255, 255, 255, 255, 0, 0, 0, 4, 240, 159, 145, 185]),
            out.to_byte_array(),
        )
        self.assertEqual(17, out.position())

    def test_write_utf(self):
        out = _ObjectDataOutput(10, None)
        out.write_utf("a")
        out.write_utf(None)
        out.write_utf("👹")
        self.assertEqual(
            bytearray([0, 0, 0, 1, 97, 255, 255, 255, 255, 0, 0, 0, 4, 240, 159, 145, 185]),
            out.to_byte_array(),
        )
        self.assertEqual(17, out.position())

    def test_write_bytes(self):
        out = _ObjectDataOutput(10, None)
        out.write_bytes("a")
        out.write_bytes("bc")
        self.assertEqual(
            bytearray([97, 98, 99]),
            out.to_byte_array(),
        )
        self.assertEqual(3, out.position())

    def test_write_chars(self):
        out = _ObjectDataOutput(10, None)
        out.write_chars("a")
        out.write_chars("bc")
        self.assertEqual(
            bytearray([0, 97, 0, 98, 0, 99]),
            out.to_byte_array(),
        )
        self.assertEqual(6, out.position())

    def test_write_byte_array(self):
        out = _ObjectDataOutput(10, None)
        out.write_byte_array(bytearray(range(10)))
        out.write_byte_array(None)
        self.assertEqual(
            bytearray([0, 0, 0, 10] + list(range(10)) + [255, 255, 255, 255]),
            out.to_byte_array(),
        )
        self.assertEqual(18, out.position())

    def test_write_signed_byte_array(self):
        out = _ObjectDataOutput(1, None)
        out.write_signed_byte_array([-1, 127, -128])
        out.write_signed_byte_array(None)
        self.assertEqual(
            bytearray([0, 0, 0, 3, 255, 127, 128, 255, 255, 255, 255]), out.to_byte_array()
        )
        self.assertEqual(11, out.position())

    def test_write_boolean_array(self):
        out = _ObjectDataOutput(1, None)
        out.write_boolean_array([True, False, True])
        out.write_boolean_array(None)
        self.assertEqual(bytearray([0, 0, 0, 3, 1, 0, 1, 255, 255, 255, 255]), out.to_byte_array())
        self.assertEqual(11, out.position())

    def test_write_char_array(self):
        out = _ObjectDataOutput(1, None)
        out.write_char_array(["a", "b"])
        out.write_char_array(None)
        self.assertEqual(
            bytearray([0, 0, 0, 2, 0, 97, 0, 98, 255, 255, 255, 255]), out.to_byte_array()
        )
        self.assertEqual(12, out.position())

    def test_write_short_array(self):
        out = _ObjectDataOutput(1, None)
        out.write_short_array([-1, 23456, -23456])
        out.write_short_array(None)
        self.assertEqual(
            bytearray([0, 0, 0, 3, 255, 255, 91, 160, 164, 96, 255, 255, 255, 255]),
            out.to_byte_array(),
        )
        self.assertEqual(14, out.position())

    def test_write_int_array(self):
        out = _ObjectDataOutput(1, None)
        out.write_int_array([-1])
        out.write_int_array(None)
        self.assertEqual(
            bytearray([0, 0, 0, 1, 255, 255, 255, 255, 255, 255, 255, 255]), out.to_byte_array()
        )
        self.assertEqual(12, out.position())

    def test_write_long_array(self):
        out = _ObjectDataOutput(1, None)
        out.write_long_array([1234])
        out.write_long_array(None)
        self.assertEqual(
            bytearray([0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 4, 210, 255, 255, 255, 255]),
            out.to_byte_array(),
        )
        self.assertEqual(16, out.position())

    def test_write_float_array(self):
        out = _ObjectDataOutput(1, None)
        out.write_float_array(None)
        out.write_float_array([42.5, -442.4])
        self.assertEqual(
            bytearray([255, 255, 255, 255, 0, 0, 0, 2, 66, 42, 0, 0, 195, 221, 51, 51]),
            out.to_byte_array(),
        )
        self.assertEqual(16, out.position())

    def test_write_double_array(self):
        out = _ObjectDataOutput(1, None)
        out.write_double_array(None)
        out.write_double_array([12345.6789])
        self.assertEqual(
            bytearray([255, 255, 255, 255, 0, 0, 0, 1, 64, 200, 28, 214, 230, 49, 248, 161]),
            out.to_byte_array(),
        )
        self.assertEqual(16, out.position())

    def test_write_string_array(self):
        out = _ObjectDataOutput(1, None)
        out.write_string_array(["a", "👹"])
        out.write_string_array(None)
        self.assertEqual(
            bytearray(
                [0, 0, 0, 2, 0, 0, 0, 1, 97, 0, 0, 0, 4, 240, 159, 145, 185, 255, 255, 255, 255]
            ),
            out.to_byte_array(),
        )
        self.assertEqual(21, out.position())

    def test_write_utf_array(self):
        out = _ObjectDataOutput(1, None)
        out.write_utf_array(["a", "👹"])
        out.write_utf_array(None)
        self.assertEqual(
            bytearray(
                [0, 0, 0, 2, 0, 0, 0, 1, 97, 0, 0, 0, 4, 240, 159, 145, 185, 255, 255, 255, 255]
            ),
            out.to_byte_array(),
        )
        self.assertEqual(21, out.position())
