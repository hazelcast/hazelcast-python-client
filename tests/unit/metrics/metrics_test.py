import unittest

from os import path

from hazelcast.metrics import (
    MetricsCompressor,
    MetricDescriptor,
    ProbeUnit,
    _OutputBuffer,
    _MetricsDictionary,
)


class MetricsCompatibilityTest(unittest.TestCase):
    def test_compatibility(self):
        here = path.abspath(path.dirname(__file__))
        with open(path.join(here, "metrics.compatibility.binary"), "rb") as f:
            expected_blob = f.read()

        compressor = MetricsCompressor()

        compressor.add_long(
            MetricDescriptor(
                metric="deltaMetric1",
                prefix="prefix",
                discriminator="ds",
                discriminator_value="dsName1",
                unit=ProbeUnit.COUNT,
            ),
            42,
        )

        compressor.add_double(
            MetricDescriptor(
                metric="deltaMetric2",
                prefix="prefix",
                discriminator="ds",
                discriminator_value="dsName1",
                unit=ProbeUnit.COUNT,
            ),
            -4.2,
        )

        compressor.add_long(
            MetricDescriptor(
                metric="longPrefixMetric",
                prefix="a" * 254,
                unit=ProbeUnit.BYTES,
            ),
            2147483647,
        )

        actual_blob = compressor.generate_blob()
        self.assertEqual(expected_blob, actual_blob)


class OutputBufferTest(unittest.TestCase):
    def test_output_buffer_growth(self):
        out = _OutputBuffer()
        size = 10 * 1024
        for i in range(size):
            out.write_byte(i % 256)

        self.assertEqual(size, len(out.to_bytearray()))

    def test_output_buffer_growth_with_size(self):
        out = _OutputBuffer(1024)
        size = 10 * 1024
        for i in range(size):
            out.write_byte(i % 256)

        self.assertEqual(size, len(out.to_bytearray()))

    def test_write_bytearray(self):
        out = _OutputBuffer()
        arr = bytearray(range(256))
        out.write_bytearray(arr)

        self.assertEqual(arr, out.to_bytearray())

    def test_write_byte(self):
        out = _OutputBuffer()
        out.write_byte(42)

        self.assertEqual(bytearray([0x2A]), out.to_bytearray())

    def test_write_char(self):
        out = _OutputBuffer()
        out.write_char("z")

        self.assertEqual(bytearray([0x00, 0x7A]), out.to_bytearray())

    def test_write_double(self):
        out = _OutputBuffer()
        out.write_double(42.5)

        self.assertEqual(
            bytearray([0x40, 0x45, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00]), out.to_bytearray()
        )

    def test_write_int(self):
        out = _OutputBuffer()
        out.write_int(42)

        self.assertEqual(bytearray([0x00, 0x00, 0x00, 0x2A]), out.to_bytearray())

    def test_write_long(self):
        out = _OutputBuffer()
        out.write_long(42)

        self.assertEqual(
            bytearray([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A]), out.to_bytearray()
        )

    def test_compress(self):
        out = _OutputBuffer()
        size = 10 * 1024
        for i in range(size):
            out.write_byte(0)

        self.assertTrue(len(out.compress()) < size)


class MetricsDictionaryTest(unittest.TestCase):
    def test_get_dict_id(self):
        d = _MetricsDictionary()

        for i in range(100):
            self.assertEqual(i, d.get_dict_id("w-%s" % i))

    def test_get_dict_id_with_long_word(self):
        d = _MetricsDictionary()

        with self.assertRaises(ValueError):
            d.get_dict_id("a" * 1000)

    def test_get_dict_id_with_the_same_word(self):
        d = _MetricsDictionary()

        word_id = d.get_dict_id("w")
        self.assertNotEqual(word_id, d.get_dict_id("w2"))
        self.assertNotEqual(word_id, d.get_dict_id("w3"))
        self.assertEqual(word_id, d.get_dict_id("w"))

    def test_get_words(self):
        d = _MetricsDictionary()

        d.get_dict_id("3")
        d.get_dict_id("2")
        d.get_dict_id("1")

        words = d.get_words()
        self.assertEqual(3, len(words))
        for i, word in enumerate(words, 1):
            self.assertEqual(str(i), word.word)
            self.assertEqual(3 - i, word.dict_id)

    def test_get_words_with_empty_dict(self):
        d = _MetricsDictionary()

        words = d.get_words()
        self.assertEqual(0, len(words))
