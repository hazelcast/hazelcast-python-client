import unittest
import binascii

from hazelcast.serialization.output import *


class OutputTestCase(unittest.TestCase):
    def setUp(self):
        self._output = ObjectDataOutput(100, None, BIG_ENDIAN)
        self.BOOL_ARR = [False, True, True, True]
        self.INT_ARR = [1, 2, 3, 4]

    def test_bool_array(self):
        initial_pos = self._output._pos
        self._output.write_boolean_array(self.BOOL_ARR)
        self.assertEqual(bytearray(binascii.unhexlify("00000004")), self._output._buffer[initial_pos:initial_pos + 4])
        self.assertEqual(bytearray(self.BOOL_ARR), self._output._buffer[initial_pos + 4:initial_pos + 8])

    def test_int_array(self):
        initial_pos = self._output._pos
        self._output.write_int_array(self.INT_ARR)
        self.assertEqual(bytearray(binascii.unhexlify("00000004")), self._output._buffer[initial_pos:initial_pos + 4])
        self.assertEqual(bytearray(self.INT_ARR), self._output._buffer[initial_pos + 4:initial_pos + 20])


if __name__ == '__main__':
    unittest.main()
