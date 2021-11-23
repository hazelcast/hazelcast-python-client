# coding=utf-8
import binascii
import unittest

from hazelcast.config import _Config
from hazelcast.serialization.bits import *
from hazelcast.serialization.data import Data
from hazelcast.serialization.serialization_const import CONSTANT_TYPE_STRING
from hazelcast.serialization.service import SerializationServiceV1

TEST_DATA_TURKISH = "Pijamalı hasta, yağız şoföre çabucak güvendi."
TEST_DATA_JAPANESE = "イロハニホヘト チリヌルヲ ワカヨタレソ ツネナラム"
TEST_DATA_ASCII = "The quick brown fox jumps over the lazy dog"
TEST_DATA_ALL = TEST_DATA_TURKISH + TEST_DATA_JAPANESE + TEST_DATA_ASCII
TEST_STR_SIZE = 1 << 20
TEST_DATA_BYTES_ALL = TEST_DATA_ALL.encode("utf8")


def to_data_byte(inp):
    encoded_data = inp.encode("utf8")

    # 4 byte partition hashcode -  4 byte of type id - 4 byte string length
    bf = bytearray(12)
    BE_INT.pack_into(bf, 0, 0)
    BE_INT.pack_into(bf, 4, CONSTANT_TYPE_STRING)
    BE_INT.pack_into(bf, 8, len(encoded_data))
    return bf + encoded_data


class StringSerializationTestCase(unittest.TestCase):
    def setUp(self):
        self.service = SerializationServiceV1(_Config())

    def test_ascii_encode(self):
        data_byte = to_data_byte(TEST_DATA_ASCII)
        expected = binascii.hexlify(data_byte)
        data = self.service.to_data(TEST_DATA_ASCII)
        actual = binascii.hexlify(data._buffer)
        self.assertEqual(expected, actual)

    def test_ascii_decode(self):
        data_byte = to_data_byte(TEST_DATA_ASCII)
        data = Data(data_byte)
        actual_ascii = self.service.to_object(data)
        self.assertEqual(TEST_DATA_ASCII, actual_ascii)

    def test_utf8_encode(self):
        data_byte = to_data_byte(TEST_DATA_ALL)
        expected = binascii.hexlify(data_byte)
        data = self.service.to_data(TEST_DATA_ALL)
        actual = binascii.hexlify(data._buffer)
        self.assertEqual(expected, actual)

    def test_utf8_decode(self):
        data_byte = to_data_byte(TEST_DATA_ALL)
        data = Data(data_byte)
        actual_ascii = self.service.to_object(data)
        self.assertEqual(TEST_DATA_ALL, actual_ascii)

    def test_None_str_encode_decode(self):
        none_str = self.service.to_data(None)
        decoded = self.service.to_object(none_str)
        self.assertIsNone(decoded)
