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