from hazelcast.config import (
    IndexConfig,
    IndexUtil,
    IndexType,
    QueryConstants,
    UniqueKeyTransformation,
)
from hazelcast.util import calculate_version
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
