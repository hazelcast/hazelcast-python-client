import unittest
import binascii

from hazelcast.serialization.input import _ObjectDataInput


class InputTestCase(unittest.TestCase):
    def setUp(self):
        self.BOOL_ARR = [False, True, True, True]
        self.INT_ARR = [1, 2, 3, 4]
        self.SHORT_ARR = [1, 2, 3, 4]

    def test_bool_array(self):
        buff = bytearray(binascii.unhexlify("0000000400010101"))
        _input = _ObjectDataInput(buff, 0, None, True)
        initial_pos = _input._pos
        read_arr = _input.read_boolean_array()
        self.assertEqual(0, initial_pos)
        self.assertEqual(self.BOOL_ARR, read_arr)

    def test_int_array(self):
        buff = bytearray(binascii.unhexlify("0000000400000001000000020000000300000004"))
        _input = _ObjectDataInput(buff, 0, None, True)
        initial_pos = _input._pos
        read_arr = _input.read_int_array()
        self.assertEqual(0, initial_pos)
        self.assertEqual(self.INT_ARR, read_arr)

    def test_short_array(self):
        buff = bytearray(binascii.unhexlify("000000040001000200030004"))
        _input = _ObjectDataInput(buff, 0, None, True)
        initial_pos = _input._pos
        read_arr = _input.read_short_array()
        self.assertEqual(0, initial_pos)
        self.assertEqual(self.INT_ARR, read_arr)

    def test_char_be(self):
        buff = bytearray(binascii.unhexlify("00e70000"))
        _input = _ObjectDataInput(buff, 0, None, True)
        initial_pos = _input._pos
        char = _input.read_char()
        self.assertEqual(0, initial_pos)
        self.assertEqual(unichr(0x00e7), char)

    def test_char_le(self):
        buff = bytearray(binascii.unhexlify("e7000000"))
        _input = _ObjectDataInput(buff, 0, None, False)
        initial_pos = _input._pos
        char = _input.read_char()
        self.assertEqual(0, initial_pos)
        self.assertEqual(unichr(0x00e7), char)


if __name__ == '__main__':
    unittest.main()
