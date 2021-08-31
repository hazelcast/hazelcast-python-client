from parameterized import parameterized

from hazelcast.config import (
    IndexConfig,
    IndexUtil,
    IndexType,
    QueryConstants,
    UniqueKeyTransformation,
)
from hazelcast.util import calculate_version, int_from_bytes, int_to_bytes
from unittest import TestCase


class VersionUtilTest(TestCase):
    def test_version_string(self):
        self.assertEqual(-1, calculate_version(""))
        self.assertEqual(-1, calculate_version("a.3.7.5"))
        self.assertEqual(-1, calculate_version("3.a.5"))
        self.assertEqual(-1, calculate_version("3,7.5"))
        self.assertEqual(-1, calculate_version("3.7,5"))
        self.assertEqual(-1, calculate_version("10.99.RC1"))
        self.assertEqual(30702, calculate_version("3.7.2"))
        self.assertEqual(19930, calculate_version("1.99.30"))
        self.assertEqual(30700, calculate_version("3.7-SNAPSHOT"))
        self.assertEqual(30702, calculate_version("3.7.2-SNAPSHOT"))
        self.assertEqual(109902, calculate_version("10.99.2-SNAPSHOT"))
        self.assertEqual(109930, calculate_version("10.99.30-SNAPSHOT"))
        self.assertEqual(109900, calculate_version("10.99-RC1"))


class IndexUtilTest(TestCase):
    def test_with_no_attributes(self):
        config = IndexConfig()

        with self.assertRaises(ValueError):
            IndexUtil.validate_and_normalize("", config)

    def test_with_too_many_attributes(self):
        attributes = ["attr_%s" % i for i in range(512)]
        config = IndexConfig(attributes=attributes)

        with self.assertRaises(ValueError):
            IndexUtil.validate_and_normalize("", config)

    def test_with_composite_bitmap_indexes(self):
        config = IndexConfig(attributes=["attr1", "attr2"], type=IndexType.BITMAP)

        with self.assertRaises(ValueError):
            IndexUtil.validate_and_normalize("", config)

    def test_canonicalize_attribute_name(self):
        config = IndexConfig(attributes=["this.x.y.z", "a.b.c"])
        normalized = IndexUtil.validate_and_normalize("", config)
        self.assertEqual("x.y.z", normalized.attributes[0])
        self.assertEqual("a.b.c", normalized.attributes[1])

    def test_duplicate_attributes(self):
        invalid_attributes = [
            ["a", "b", "a"],
            ["a", "b", " a"],
            [" a", "b", "a"],
            ["this.a", "b", "a"],
            ["this.a ", "b", " a"],
            ["this.a", "b", "this.a"],
            ["this.a ", "b", " this.a"],
            [" this.a", "b", "a"],
        ]

        for attributes in invalid_attributes:
            with self.assertRaises(ValueError):
                config = IndexConfig(attributes=attributes)
                IndexUtil.validate_and_normalize("", config)

    def test_normalized_name(self):
        config = IndexConfig(None, IndexType.SORTED, ["attr"])
        normalized = IndexUtil.validate_and_normalize("map", config)
        self.assertEqual("map_sorted_attr", normalized.name)

        config = IndexConfig("test", IndexType.BITMAP, ["attr"])
        normalized = IndexUtil.validate_and_normalize("map", config)
        self.assertEqual("test", normalized.name)

        config = IndexConfig(None, IndexType.HASH, ["this.attr2.x"])
        normalized = IndexUtil.validate_and_normalize("map2", config)
        self.assertEqual("map2_hash_attr2.x", normalized.name)

    def test_with_bitmap_indexes(self):
        bio = {
            "unique_key": QueryConstants.THIS_ATTRIBUTE_NAME,
            "unique_key_transformation": UniqueKeyTransformation.RAW,
        }
        config = IndexConfig(type=IndexType.BITMAP, attributes=["attr"], bitmap_index_options=bio)
        normalized = IndexUtil.validate_and_normalize("map", config)
        self.assertEqual(bio["unique_key"], normalized.bitmap_index_options.unique_key)
        self.assertEqual(
            bio["unique_key_transformation"],
            normalized.bitmap_index_options.unique_key_transformation,
        )


# (int number, bytearray representation).
_CONVERSION_TEST_CASES = [
    (0, [0]),
    (-1, [255]),
    (127, [127]),
    (-128, [128]),
    (999, [3, 231]),
    (-123456, [254, 29, 192]),
    (2147483647, [127, 255, 255, 255]),
    (-2147483648, [128, 0, 0, 0]),
    (9223372036854775807, [127, 255, 255, 255, 255, 255, 255, 255]),
    (-9223372036854775808, [128, 0, 0, 0, 0, 0, 0, 0]),
    # fmt: off
    (
        23154266667777888899991234566543219999888877776666245132,
        [0, 241, 189, 232, 38, 131, 251, 137, 60, 34, 23, 53, 163, 116, 208, 85, 14, 65, 40, 174, 120, 10, 56, 12]
    ),
    (
        -23154266667777888899991234566543219999888877776666245132,
        [255, 14, 66, 23, 217, 124, 4, 118, 195, 221, 232, 202, 92, 139, 47, 170, 241, 190, 215, 81, 135, 245, 199, 244]
    )
    # fmt: on
]


class IntConversionTest(TestCase):
    @parameterized.expand(_CONVERSION_TEST_CASES)
    def test_int_from_bytes(self, number, buf):
        self.assertEqual(number, int_from_bytes(buf))

    @parameterized.expand(_CONVERSION_TEST_CASES)
    def test_int_to_bytes(self, number, buf):
        self.assertEqual(bytearray(buf), int_to_bytes(number))
