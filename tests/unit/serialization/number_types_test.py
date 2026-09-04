import sys
import unittest

from parameterized import parameterized

from hazelcast import Int32, Int8, Int16, Int64, BigInt, Float32, Float64


class NumberTypesTest(unittest.TestCase):

    @parameterized.expand(
        [
            (Int8(8), Int8(8), True),
            (Int8(8), Int8(20), False),
            (Int8(8), Int32(8), False),
            (Int8(8), 8, False),
            (Int16(16), Int16(16), True),
            (Int16(16), Int16(20), False),
            (Int16(16), Int32(16), False),
            (Int16(16), 16, False),
            (Int32(32), Int32(32), True),
            (Int32(32), Int32(20), False),
            (Int32(32), Int8(32), False),
            (Int32(32), 32, True),
            (Int64(64), Int64(64), True),
            (Int64(64), Int64(20), False),
            (Int64(64), Int32(64), False),
            (Int64(64), 64, False),
            (BigInt(2 << 64 + 1), BigInt(2 << 64 + 1), True),
            (BigInt(2 << 64 + 1), BigInt(20), False),
            (BigInt(64), Int32(64), False),
            (BigInt(64), 64, False),
            (Float32(32.64), Float32(32.64), True),
            (Float32(32.64), Float32(0.16), False),
            (Float32(32.64), Float64(32.64), False),
            (Float32(32.64), 32.64, False),
            (Float64(64.32), Float64(64.32), True),
            (Float64(64.32), Float64(0.16), False),
            (Float64(64.32), Float32(64.32), False),
            (Float64(64.32), 64.32, True),
        ]
    )
    def test_equality(self, a, b, is_equal):
        if is_equal:
            self.assertEqual(a, b)
        else:
            self.assertNotEqual(a, b)

    @parameterized.expand(
        [
            (Int8(1), Int8(1), Int8(3), {Int8(1): 2, Int8(3): 3}),
            (Int16(1), Int16(1), Int16(3), {Int16(1): 2, Int16(3): 3}),
            (Int32(1), Int32(1), Int32(3), {Int32(1): 2, Int32(3): 3}),
            (Int64(1), Int64(1), Int64(3), {Int64(1): 2, Int64(3): 3}),
            (BigInt(1), BigInt(1), BigInt(3), {BigInt(1): 2, BigInt(3): 3}),
            (Float32(1), Float32(1), Float32(3), {Float32(1): 2, Float32(3): 3}),
            (Float64(1), Float64(1), Float64(3), {Float64(1): 2, Float64(3): 3}),
        ]
    )
    def test_dict_item(self, a, b, c, result):
        d = {
            a: 1,
            b: 2,
            c: 3,
        }
        self.assertEqual(d, result)

    @parameterized.expand(
        [
            (1.100, True),
            (-1.100, True),
            (1e100, False),
            (-1e100, False),
        ]
    )
    def test_float32_range(self, value, ok):
        if (sys.version_info.major, sys.version_info.minor) < (3, 14):
            self.skipTest("OverflowError is only raised for Python 3.14 and up")
        if ok:
            Float32(value)
        else:
            self.assertRaises(ValueError, lambda: Float32(value))

    @parameterized.expand([(1.100,), (-1.100,), (1e100,), (-1e100,)])
    def test_float64_range(self, value):
        Float64(value)
