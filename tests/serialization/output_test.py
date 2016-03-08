import binascii
import unittest

from hazelcast.serialization.output import _ObjectDataOutput


class OutputTestCase(unittest.TestCase):
    def setUp(self):
        self._output = _ObjectDataOutput(100, None, True)
        self.BOOL_ARR = [False, True, True, True]
        self.INT_ARR = [1, 2, 3, 4]

    def test_bool_array(self):
        initial_pos = self._output._pos
        self._output.write_boolean_array(self.BOOL_ARR)
        self.assertEqual(bytearray(binascii.unhexlify("00000004")), self._output._buffer[initial_pos:initial_pos + 4])
        self.assertEqual(bytearray(binascii.unhexlify("00010101")), self._output._buffer[initial_pos + 4:initial_pos + 8])

    def test_int_array(self):
        pos = self._output._pos
        self._output.write_int_array(self.INT_ARR)
        self.assertEqual(bytearray(binascii.unhexlify("00000004")),  self._output._buffer[pos:pos + 4])
        self.assertEqual(bytearray(binascii.unhexlify("00000001000000020000000300000004")),  self._output._buffer[pos+4:pos + 20])

    def test_char(self):
        pos = self._output._pos
        self._output.write_char(unichr(0x00e7))
        self.assertEqual(bytearray(binascii.unhexlify("00e70000000000000000")),  self._output._buffer[pos:pos + 10])


if __name__ == '__main__':
    unittest.main()
