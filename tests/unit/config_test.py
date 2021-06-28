import unittest

from hazelcast.config import (
    _Config,
    SSLProtocol,
    ReconnectMode,
    IntType,
    InMemoryFormat,
    EvictionPolicy,
    IndexConfig,
    IndexType,
    UniqueKeyTransformation,
    QueryConstants,
    BitmapIndexOptions,
    TopicOverloadPolicy,
)
from hazelcast.errors import InvalidConfigurationError
from hazelcast.serialization.api import IdentifiedDataSerializable, Portable, StreamSerializer
from hazelcast.serialization.portable.classdef import ClassDefinition
from hazelcast.util import RandomLB


class ConfigTest(unittest.TestCase):
    def setUp(self):
        self.config = _Config()

    def test_from_dict_defaults(self):
        config = _Config.from_dict({})
        for item in self.config.__slots__:
            self.assertEqual(getattr(self.config, item), getattr(config, item))

    def test_from_dict_with_a_few_changes(self):
        config = _Config.from_dict({"client_name": "hazel", "cluster_name": "cast"})
        for item in self.config.__slots__:
            if item == "_client_name" or item == "_cluster_name":
                continue
            self.assertEqual(getattr(self.config, item), getattr(config, item))

        self.assertEqual("hazel", config.client_name)
        self.assertEqual("cast", config.cluster_name)

    def test_from_dict_skip_none_item(self):
        config = _Config.from_dict({"cluster_name": None, "cluster_members": None})
        for item in self.config.__slots__:
            self.assertEqual(getattr(self.config, item), getattr(config, item))

    def test_from_dict_with_invalid_elements(self):
        with self.assertRaises(InvalidConfigurationError):
            _Config.from_dict({"invalid_elem": False})

    def test_cluster_members(self):
        config = self.config
        self.assertEqual([], config.cluster_members)

        with self.assertRaises(TypeError):
            config.cluster_members = [1]

        with self.assertRaises(TypeError):
            config.cluster_members = 1

        addresses = ["localhost", "10.162.1.1:5701"]
        config.cluster_members = addresses
        self.assertEqual(addresses, config.cluster_members)

    def test_cluster_name(self):
        config = self.config
        self.assertEqual("dev", config.cluster_name)

        with self.assertRaises(TypeError):
            config.cluster_name = 1

        config.cluster_name = "xyz"
        self.assertEqual("xyz", config.cluster_name)

    def test_client_name(self):
        config = self.config
        self.assertIsNone(config.client_name)

        with self.assertRaises(TypeError):
            config.client_name = None

        config.client_name = "xyz"
        self.assertEqual("xyz", config.client_name)
