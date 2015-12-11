import unittest
import binascii

from hazelcast.serialization.input import ObjectDataInput


class InputTestCase(unittest.TestCase):
    def setUp(self):
        self.BOOL_ARR = [False, True, True, True]
        self.INT_ARR = [1, 2, 3, 4]

    def test_bool_array(self):
        buff = bytearray(binascii.unhexlify("00000004")) + bytearray(self.BOOL_ARR)
        _input = ObjectDataInput(buff, None)
        initial_pos = _input._pos
        read_arr = _input.read_boolean_array()
        self.assertEqual(0, initial_pos)
        self.assertEqual(self.BOOL_ARR, read_arr)

    def test_int_array(self):
        buff = bytearray(binascii.unhexlify("0000000400000001000000020000000300000004"))
        _input = ObjectDataInput(buff, None)
        initial_pos = _input._pos
        read_arr = _input.read_int_array()
        self.assertEqual(0, initial_pos)
        self.assertEqual(self.INT_ARR, read_arr)


if __name__ == '__main__':
    unittest.main()
