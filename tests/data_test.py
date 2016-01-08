import binascii
import unittest

from hazelcast.serialization.data import *


class DataTestCase(unittest.TestCase):
    def setUp(self):
        #    PARTITION HASH -    TYPE    -      DATA
        hexstr = "12345678" + "01020304" + "12345678"
        buff = binascii.unhexlify(hexstr)
        self._data = Data(buff)
        self._total_size = len(hexstr) / 2

    def tearDown(self):
        del self._data

    def test_data(self):
        self.assertEqual(self._total_size, self._data.total_size())
        self.assertEqual(self._total_size - DATA_OFFSET, self._data.data_size())
        self.assertEqual(0x01020304, self._data.get_type())
        self.assertTrue(self._data.has_partition_hash())
        self.assertFalse(self._data.is_portable())
        self.assertEqual(1545424565, self._data.hash_code())
        self.assertEqual(0x12345678, self._data.get_partition_hash())

    def test_data_len(self):
        self.assertEqual(10, len(Data("1"* 10)))

if __name__ == '__main__':
    unittest.main()
