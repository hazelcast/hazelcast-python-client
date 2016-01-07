# coding=utf-8
import binascii
import struct
import unittest

from hazelcast.config import SerializationConfig
from hazelcast.serialization.bits import *
from hazelcast.serialization.data import Data
from hazelcast.serialization.serialization_const import CONSTANT_TYPE_STRING
from hazelcast.serialization.service import SerializationServiceV1

TEST_DATA_TURKISH = u"Pijamalı hasta, yağız şoföre çabucak güvendi."
TEST_DATA_JAPANESE = u"イロハニホヘト チリヌルヲ ワカヨタレソ ツネナラム"
TEST_DATA_ASCII = "The quick brown fox jumps over the lazy dog"
TEST_DATA_ALL = TEST_DATA_TURKISH + TEST_DATA_JAPANESE + TEST_DATA_ASCII
TEST_STR_SIZE = 1 << 20
TEST_DATA_BYTES_ALL = TEST_DATA_ALL.encode("utf8")


def to_data_byte(inp, length):
    # 4 byte partition hashcode -  4 byte of type id - 4 byte string length
    bf = bytearray(12)
    struct.pack_into(FMT_BE_INT, bf, 0, 0)
    struct.pack_into(FMT_BE_INT, bf, 4, CONSTANT_TYPE_STRING)
    struct.pack_into(FMT_BE_INT, bf, 8, length)
    return bf + bytearray(inp.encode("utf-8"))


class StringSerializationTestCase(unittest.TestCase):
    def setUp(self):
        self.service = SerializationServiceV1(serialization_config=SerializationConfig())

    def test_ascii_encode(self):
        data_byte = to_data_byte(TEST_DATA_ASCII, len(TEST_DATA_ASCII))
        expected = binascii.hexlify(data_byte)
        data = self.service.to_data(TEST_DATA_ASCII)
        actual = binascii.hexlify(data._buffer)
        self.assertEqual(expected, actual)

    def test_ascii_decode(self):
        data_byte = to_data_byte(TEST_DATA_ASCII, len(TEST_DATA_ASCII))
        data = Data(buff=data_byte)
        actual_ascii = self.service.to_object(data)
        self.assertEqual(TEST_DATA_ASCII, actual_ascii)

    def test_utf8_encode(self):
        data_byte = to_data_byte(TEST_DATA_ALL, len(TEST_DATA_ALL))
        expected = binascii.hexlify(data_byte)
        data = self.service.to_data(TEST_DATA_ALL)
        actual = binascii.hexlify(data._buffer)
        self.assertEqual(expected, actual)

    def test_utf8_decode(self):
        data_byte = to_data_byte(TEST_DATA_ALL, len(TEST_DATA_ALL))
        data = Data(buff=data_byte)
        actual_ascii = self.service.to_object(data)
        self.assertEqual(TEST_DATA_ALL, actual_ascii)

    def test_None_str_encode_decode(self):
        none_str = self.service.to_data(None)
        decoded = self.service.to_object(none_str)
        self.assertIsNone(decoded)


if __name__ == '__main__':
    unittest.main()
