import unittest

from hazelcast.serialization.input import _ObjectDataInput


class InputTestCase(unittest.TestCase):
    def test_read_into(self):
        b = bytearray(range(10))
        inp = _ObjectDataInput(b)
        into = bytearray(10)
        inp.read_into(into, 5, 3)
        self.assertEqual(b[:3], into[5 : 5 + 3])

    def test_read_into_when_length_greater_than_remaining(self):
        b = bytearray(range(10))
        inp = _ObjectDataInput(b, offset=5)
        into = bytearray(10)
        inp.read_into(into, length=10)
        self.assertEqual(b[5:], into[:5])

    def test_skip_bytes(self):
        inp = _ObjectDataInput(bytearray(10))
        self.assertEqual(0, inp.position())
        self.assertEqual(4, inp.skip_bytes(4))
        self.assertEqual(4, inp.position())

    def test_skip_bytes_when_count_greater_than_remaining(self):
        inp = _ObjectDataInput(bytearray(10))
        inp.set_position(8)
        self.assertEqual(2, inp.skip_bytes(4))
        self.assertEqual(10, inp.position())

    def test_skip_bytes_when_count_is_not_positive(self):
        inp = _ObjectDataInput(bytearray(10))
        self.assertEqual(0, inp.skip_bytes(0))
        self.assertEqual(0, inp.position())
        self.assertEqual(0, inp.skip_bytes(-1))
        self.assertEqual(0, inp.position())

    def test_read_boolean(self):
        inp = _ObjectDataInput(bytearray([0, 1, 0]))
        self.assertFalse(inp.read_boolean())
        self.assertTrue(inp.read_boolean())
        self.assertFalse(inp.read_boolean())
        self.assertEqual(3, inp.position())

        with self.assertRaises(EOFError):
            inp.read_boolean()

    def test_read_boolean_positional(self):
        inp = _ObjectDataInput(bytearray([0, 1, 0]))
        self.assertFalse(inp.read_boolean_positional(0))
        self.assertTrue(inp.read_boolean_positional(1))
        self.assertFalse(inp.read_boolean_positional(2))
        self.assertEqual(0, inp.position())

        with self.assertRaises(EOFError):
            inp.read_boolean_positional(100)

    def test_read_byte(self):
        inp = _ObjectDataInput(bytearray([0, 255, 127, 128, 42]))
        self.assertEqual(0, inp.read_byte())
        self.assertEqual(-1, inp.read_byte())
        self.assertEqual(127, inp.read_byte())
        self.assertEqual(-128, inp.read_byte())
        self.assertEqual(42, inp.read_byte())
        self.assertEqual(5, inp.position())

        with self.assertRaises(EOFError):
            inp.read_byte()

    def test_read_byte_positional(self):
        inp = _ObjectDataInput(bytearray([0, 255, 127, 128, 42]))
        self.assertEqual(0, inp.read_byte_positional(0))
        self.assertEqual(-1, inp.read_byte_positional(1))
        self.assertEqual(127, inp.read_byte_positional(2))
        self.assertEqual(-128, inp.read_byte_positional(3))
        self.assertEqual(42, inp.read_byte_positional(4))
        self.assertEqual(0, inp.position())

        with self.assertRaises(EOFError):
            inp.read_boolean_positional(100)

    def test_read_unsigned_byte(self):
        inp = _ObjectDataInput(bytearray([0, 255, 127, 128, 42]))
        self.assertEqual(0, inp.read_unsigned_byte())
        self.assertEqual(255, inp.read_unsigned_byte())
        self.assertEqual(127, inp.read_unsigned_byte())
        self.assertEqual(128, inp.read_unsigned_byte())
        self.assertEqual(42, inp.read_unsigned_byte())
        self.assertEqual(5, inp.position())

        with self.assertRaises(EOFError):
            inp.read_unsigned_byte()

    def test_read_char(self):
        inp = _ObjectDataInput(bytearray([0, 97, 10, 42, 0, 49]))
        self.assertEqual("a", inp.read_char())
        self.assertEqual("ਪ", inp.read_char())
        self.assertEqual("1", inp.read_char())
        self.assertEqual(6, inp.position())

        with self.assertRaises(EOFError):
            inp.read_char()

    def test_read_char_positional(self):
        inp = _ObjectDataInput(bytearray([0, 97, 10, 42, 0, 49]))
        self.assertEqual("a", inp.read_char_positional(0))
        self.assertEqual("ਪ", inp.read_char_positional(2))
        self.assertEqual("1", inp.read_char_positional(4))
        self.assertEqual(0, inp.position())

        with self.assertRaises(EOFError):
            inp.read_char_positional(100)

    def test_read_short(self):
        inp = _ObjectDataInput(bytearray([91, 160, 164, 96, 0, 49]))
        self.assertEqual(23456, inp.read_short())
        self.assertEqual(-23456, inp.read_short())
        self.assertEqual(49, inp.read_short())
        self.assertEqual(6, inp.position())

        with self.assertRaises(EOFError):
            inp.read_short()

    def test_read_short_positional(self):
        inp = _ObjectDataInput(bytearray([91, 160, 164, 96, 0, 49]))
        self.assertEqual(23456, inp.read_short_positional(0))
        self.assertEqual(-23456, inp.read_short_positional(2))
        self.assertEqual(49, inp.read_short_positional(4))
        self.assertEqual(0, inp.position())

        with self.assertRaises(EOFError):
            inp.read_short_positional(100)

    def test_read_unsigned_short(self):
        inp = _ObjectDataInput(bytearray([91, 160, 164, 96, 0, 49]))
        self.assertEqual(23456, inp.read_unsigned_short())
        self.assertEqual(42080, inp.read_unsigned_short())
        self.assertEqual(49, inp.read_unsigned_short())
        self.assertEqual(6, inp.position())

        with self.assertRaises(EOFError):
            inp.read_unsigned_short()

    def test_read_int(self):
        inp = _ObjectDataInput(bytearray([127, 255, 255, 255, 128, 0, 0, 0, 0, 0, 0, 42]))
        self.assertEqual(2147483647, inp.read_int())
        self.assertEqual(-2147483648, inp.read_int())
        self.assertEqual(42, inp.read_int())
        self.assertEqual(12, inp.position())

        with self.assertRaises(EOFError):
            inp.read_int()

    def test_read_int_positional(self):
        inp = _ObjectDataInput(bytearray([127, 255, 255, 255, 128, 0, 0, 0, 0, 0, 0, 42]))
        self.assertEqual(2147483647, inp.read_int_positional(0))
        self.assertEqual(-2147483648, inp.read_int_positional(4))
        self.assertEqual(42, inp.read_int_positional(8))
        self.assertEqual(0, inp.position())

        with self.assertRaises(EOFError):
            inp.read_int_positional(100)

    def test_read_long(self):
        inp = _ObjectDataInput(
            bytearray([127, 255, 255, 255, 255, 255, 255, 255, 128, 0, 0, 0, 0, 0, 0, 0])
        )
        self.assertEqual(9223372036854775807, inp.read_long())
        self.assertEqual(-9223372036854775808, inp.read_long())
        self.assertEqual(16, inp.position())

        with self.assertRaises(EOFError):
            inp.read_long()

    def test_read_long_positional(self):
        inp = _ObjectDataInput(
            bytearray([127, 255, 255, 255, 255, 255, 255, 255, 128, 0, 0, 0, 0, 0, 0, 0])
        )
        self.assertEqual(9223372036854775807, inp.read_long_positional(0))
        self.assertEqual(-9223372036854775808, inp.read_long_positional(8))
        self.assertEqual(0, inp.position())

        with self.assertRaises(EOFError):
            inp.read_long_positional(100)

    def test_read_float(self):
        inp = _ObjectDataInput(bytearray([66, 42, 0, 0, 195, 221, 51, 51, 73, 116, 30, 80]))
        self.assertAlmostEqual(42.5, inp.read_float(), delta=0.00001)
        self.assertAlmostEqual(-442.4, inp.read_float(), delta=0.00001)
        self.assertAlmostEqual(999909, inp.read_float(), delta=0.00001)
        self.assertEqual(12, inp.position())

        with self.assertRaises(EOFError):
            inp.read_float()

    def test_read_float_positional(self):
        inp = _ObjectDataInput(bytearray([66, 42, 0, 0, 195, 221, 51, 51, 73, 116, 30, 80]))
        self.assertAlmostEqual(42.5, inp.read_float_positional(0), delta=0.00001)
        self.assertAlmostEqual(-442.4, inp.read_float_positional(4), delta=0.00001)
        self.assertAlmostEqual(999909, inp.read_float_positional(8), delta=0.00001)
        self.assertEqual(0, inp.position())

        with self.assertRaises(EOFError):
            inp.read_float_positional(100)

    def test_read_double(self):
        inp = _ObjectDataInput(
            bytearray([64, 200, 28, 214, 230, 49, 248, 161, 192, 248, 28, 214, 233, 225, 176, 138])
        )
        self.assertAlmostEqual(12345.6789, inp.read_double(), delta=0.00001)
        self.assertAlmostEqual(-98765.4321, inp.read_double(), delta=0.00001)
        self.assertEqual(16, inp.position())

        with self.assertRaises(EOFError):
            inp.read_double()

    def test_read_double_positional(self):
        inp = _ObjectDataInput(
            bytearray([64, 200, 28, 214, 230, 49, 248, 161, 192, 248, 28, 214, 233, 225, 176, 138])
        )
        self.assertAlmostEqual(12345.6789, inp.read_double_positional(0), delta=0.00001)
        self.assertAlmostEqual(-98765.4321, inp.read_double_positional(8), delta=0.00001)
        self.assertEqual(0, inp.position())

        with self.assertRaises(EOFError):
            inp.read_double_positional(100)

    def test_read_string(self):
        inp = _ObjectDataInput(bytearray([0, 0, 0, 3, 97, 98, 99, 0, 0, 0, 4, 240, 159, 145, 185]))
        self.assertEqual("abc", inp.read_string())
        self.assertEqual("👹", inp.read_string())
        self.assertEqual(15, inp.position())

        with self.assertRaises(EOFError):
            inp.read_string()

    def test_read_utf(self):
        inp = _ObjectDataInput(bytearray([0, 0, 0, 3, 97, 98, 99, 0, 0, 0, 4, 240, 159, 145, 185]))
        self.assertEqual("abc", inp.read_utf())
        self.assertEqual("👹", inp.read_utf())
        self.assertEqual(15, inp.position())

        with self.assertRaises(EOFError):
            inp.read_utf()

    def test_read_byte_array(self):
        b = bytearray([0, 0, 0, 10] + list(range(10)))
        inp = _ObjectDataInput(b)
        self.assertEqual(b[4:], inp.read_byte_array())  # skip length
        self.assertEqual(len(b), inp.position())

        with self.assertRaises(EOFError):
            inp.read_byte_array()

    def test_read_i8_array(self):
        b = bytearray([0, 0, 0, 3, 1, 127, 255])
        inp = _ObjectDataInput(b)
        self.assertEqual([1, 127, -1], inp.read_i8_array())
        self.assertEqual(len(b), inp.position())

        with self.assertRaises(EOFError):
            inp.read_i8_array()

    def test_read_boolean_array(self):
        b = bytearray([0, 0, 0, 2, 0, 1])
        inp = _ObjectDataInput(b)
        self.assertEqual([False, True], inp.read_boolean_array())
        self.assertEqual(len(b), inp.position())

        with self.assertRaises(EOFError):
            inp.read_boolean_array()

    def test_read_char_array(self):
        b = bytearray([0, 0, 0, 2, 0, 97, 0, 49])
        inp = _ObjectDataInput(b)
        self.assertEqual(["a", "1"], inp.read_char_array())
        self.assertEqual(len(b), inp.position())

        with self.assertRaises(EOFError):
            inp.read_char_array()

    def test_read_short_array(self):
        b = bytearray([0, 0, 0, 2, 91, 160, 164, 96])
        inp = _ObjectDataInput(b)
        self.assertEqual([23456, -23456], inp.read_short_array())
        self.assertEqual(len(b), inp.position())

        with self.assertRaises(EOFError):
            inp.read_short_array()

    def test_read_int_array(self):
        b = bytearray([0, 0, 0, 2, 127, 255, 255, 255, 128, 0, 0, 0])
        inp = _ObjectDataInput(b)
        self.assertEqual([2147483647, -2147483648], inp.read_int_array())
        self.assertEqual(len(b), inp.position())

        with self.assertRaises(EOFError):
            inp.read_int_array()

    def test_read_long_array(self):
        b = bytearray([0, 0, 0, 1, 127, 255, 255, 255, 255, 255, 255, 255])
        inp = _ObjectDataInput(b)
        self.assertEqual([9223372036854775807], inp.read_long_array())
        self.assertEqual(len(b), inp.position())

        with self.assertRaises(EOFError):
            inp.read_long_array()

    def test_read_float_array(self):
        b = bytearray([0, 0, 0, 3, 66, 42, 0, 0, 195, 221, 51, 51, 73, 116, 30, 80])
        inp = _ObjectDataInput(b)
        expected = [42.5, -442.4, 999909]
        actual = inp.read_float_array()
        self.assertEqual(3, len(actual))
        for expected_item, actual_item in zip(expected, actual):
            self.assertAlmostEqual(expected_item, actual_item, delta=0.00001)

        self.assertEqual(len(b), inp.position())

        with self.assertRaises(EOFError):
            inp.read_float_array()

    def test_read_double_array(self):
        b = bytearray([0, 0, 0, 1, 64, 200, 28, 214, 230, 49, 248, 161])
        inp = _ObjectDataInput(b)
        expected = [12345.6789]
        actual = inp.read_double_array()
        self.assertEqual(1, len(actual))
        for expected_item, actual_item in zip(expected, actual):
            self.assertAlmostEqual(expected_item, actual_item, delta=0.00001)

        self.assertEqual(len(b), inp.position())

        with self.assertRaises(EOFError):
            inp.read_double_array()

    def test_read_string_array(self):
        b = bytearray([0, 0, 0, 1, 0, 0, 0, 3, 97, 98, 99])
        inp = _ObjectDataInput(b)
        self.assertEqual(["abc"], inp.read_string_array())
        self.assertEqual(len(b), inp.position())

        with self.assertRaises(EOFError):
            inp.read_string_array()

    def test_read_utf_array(self):
        b = bytearray([0, 0, 0, 1, 0, 0, 0, 3, 97, 98, 99])
        inp = _ObjectDataInput(b)
        self.assertEqual(["abc"], inp.read_utf_array())
        self.assertEqual(len(b), inp.position())

        with self.assertRaises(EOFError):
            inp.read_utf_array()
