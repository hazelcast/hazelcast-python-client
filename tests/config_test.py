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

    def test_connection_timeout(self):
        config = self.config
        self.assertEqual(5.0, config.connection_timeout)

        with self.assertRaises(ValueError):
            config.connection_timeout = -1

        with self.assertRaises(TypeError):
            config.connection_timeout = "1"

        config.connection_timeout = 3
        self.assertEqual(3, config.connection_timeout)

    def test_socket_options(self):
        config = self.config
        self.assertEqual([], config.socket_options)

        with self.assertRaises(TypeError):
            config.socket_options = [(1, 2, 3), (1, 2)]

        with self.assertRaises(TypeError):
            config.socket_options = (1, 2, 3)

        options = [(1, 2, 3), [4, 5, 6]]
        config.socket_options = options
        self.assertEqual(options, config.socket_options)

    def test_redo_operation(self):
        config = self.config
        self.assertFalse(config.redo_operation)

        with self.assertRaises(TypeError):
            config.redo_operation = "false"

        config.redo_operation = True
        self.assertTrue(config.redo_operation)

    def test_smart_routing(self):
        config = self.config
        self.assertTrue(config.smart_routing)

        with self.assertRaises(TypeError):
            config.smart_routing = None

        config.smart_routing = False
        self.assertFalse(config.smart_routing)

    def test_ssl_enabled(self):
        config = self.config
        self.assertFalse(config.ssl_enabled)

        with self.assertRaises(TypeError):
            config.ssl_enabled = 123

        config.ssl_enabled = True
        self.assertTrue(config.ssl_enabled)

    def test_ssl_cafile(self):
        config = self.config
        self.assertIsNone(config.ssl_cafile)

        with self.assertRaises(TypeError):
            config.ssl_cafile = False

        config.ssl_cafile = "/path"
        self.assertEqual("/path", config.ssl_cafile)

    def test_ssl_certfile(self):
        config = self.config
        self.assertIsNone(config.ssl_certfile)

        with self.assertRaises(TypeError):
            config.ssl_certfile = None

        config.ssl_certfile = "/path"
        self.assertEqual("/path", config.ssl_certfile)

    def test_ssl_keyfile(self):
        config = self.config
        self.assertIsNone(config.ssl_keyfile)

        with self.assertRaises(TypeError):
            config.ssl_keyfile = None

        config.ssl_keyfile = "/path"
        self.assertEqual("/path", config.ssl_keyfile)

    def test_ssl_password(self):
        config = self.config
        self.assertIsNone(config.ssl_password)

        with self.assertRaises(TypeError):
            config.ssl_password = 123

        config.ssl_password = "123"
        self.assertEqual("123", config.ssl_password)

        config.ssl_password = b"qwe"
        self.assertEqual(b"qwe", config.ssl_password)

        config.ssl_password = bytearray([1, 2, 3])
        self.assertEqual(bytearray([1, 2, 3]), config.ssl_password)

        config.ssl_password = lambda: "123"
        self.assertEqual("123", config.ssl_password())

    def test_ssl_protocol(self):
        config = self.config
        self.assertEqual(SSLProtocol.TLSv1_2, config.ssl_protocol)

        with self.assertRaises(TypeError):
            config.ssl_protocol = "123"

        config.ssl_protocol = SSLProtocol.TLSv1_3
        self.assertEqual(SSLProtocol.TLSv1_3, config.ssl_protocol)

        config.ssl_protocol = 0
        self.assertEqual(0, config.ssl_protocol)

        config.ssl_protocol = "TLSv1_2"
        self.assertEqual(SSLProtocol.TLSv1_2, config.ssl_protocol)

    def test_ssl_ciphers(self):
        config = self.config
        self.assertIsNone(config.ssl_ciphers)

        with self.assertRaises(TypeError):
            config.ssl_ciphers = 123

        config.ssl_ciphers = "123"
        self.assertEqual("123", config.ssl_ciphers)

    def test_cloud_discovery_token(self):
        config = self.config
        self.assertIsNone(config.cloud_discovery_token)

        with self.assertRaises(TypeError):
            config.cloud_discovery_token = 123

        config.cloud_discovery_token = "TOKEN"
        self.assertEqual("TOKEN", config.cloud_discovery_token)

    def test_async_start(self):
        config = self.config
        self.assertFalse(config.async_start)

        with self.assertRaises(TypeError):
            config.async_start = "false"

        config.async_start = True
        self.assertTrue(config.async_start)

    def test_reconnect_mode(self):
        config = self.config
        self.assertEqual(ReconnectMode.ON, config.reconnect_mode)

        with self.assertRaises(TypeError):
            config.reconnect_mode = None

        config.reconnect_mode = ReconnectMode.ASYNC
        self.assertEqual(ReconnectMode.ASYNC, config.reconnect_mode)

        config.reconnect_mode = 0
        self.assertEqual(0, config.reconnect_mode)

        config.reconnect_mode = "OFF"
        self.assertEqual(ReconnectMode.OFF, config.reconnect_mode)

    def test_retry_initial_backoff(self):
        config = self.config
        self.assertEqual(1, config.retry_initial_backoff)

        with self.assertRaises(ValueError):
            config.retry_initial_backoff = -100

        with self.assertRaises(TypeError):
            config.retry_initial_backoff = "123"

        config.retry_initial_backoff = 3.5
        self.assertEqual(3.5, config.retry_initial_backoff)

    def test_retry_max_backoff(self):
        config = self.config
        self.assertEqual(30, config.retry_max_backoff)

        with self.assertRaises(ValueError):
            config.retry_max_backoff = -10

        with self.assertRaises(TypeError):
            config.retry_max_backoff = None

        config.retry_initial_backoff = 0
        self.assertEqual(0, config.retry_initial_backoff)

    def test_retry_jitter(self):
        config = self.config
        self.assertEqual(0, config.retry_jitter)

        with self.assertRaises(ValueError):
            config.retry_jitter = -1

        with self.assertRaises(ValueError):
            config.retry_jitter = 1.1

        with self.assertRaises(TypeError):
            config.retry_jitter = "123"

        config.retry_jitter = 0.5
        self.assertEqual(0.5, config.retry_jitter)

    def test_retry_multiplier(self):
        config = self.config
        self.assertEqual(1.05, config.retry_multiplier)

        with self.assertRaises(ValueError):
            config.retry_multiplier = 0.5

        with self.assertRaises(TypeError):
            config.retry_multiplier = None

        config.retry_multiplier = 1.5
        self.assertEqual(1.5, config.retry_multiplier)

    def test_cluster_connect_timeout(self):
        config = self.config
        self.assertEqual(-1, config.cluster_connect_timeout)

        config.cluster_connect_timeout = -1
        self.assertEqual(-1, config.cluster_connect_timeout)

        with self.assertRaises(ValueError):
            config.cluster_connect_timeout = -2

        with self.assertRaises(TypeError):
            config.cluster_connect_timeout = ""

        config.cluster_connect_timeout = 20
        self.assertEqual(20, config.cluster_connect_timeout)

    def test_portable_version(self):
        config = self.config
        self.assertEqual(0, config.portable_version)

        with self.assertRaises(ValueError):
            config.portable_version = -1

        with self.assertRaises(TypeError):
            config.portable_version = None

        config.portable_version = 2
        self.assertEqual(2, config.portable_version)

    def test_data_serializable_factories(self):
        config = self.config
        self.assertEqual({}, config.data_serializable_factories)

        invalid_configs = [
            {"123": 1},
            {123: "123"},
            {123: {"123": 1}},
            {123: {123: "123"}},
            {123: {123: str}},
            123,
        ]

        for invalid_config in invalid_configs:
            with self.assertRaises(TypeError):
                config.data_serializable_factories = invalid_config

        factories = {1: {2: IdentifiedDataSerializable}}

        config.data_serializable_factories = factories
        self.assertEqual(factories, config.data_serializable_factories)

    def test_data_portable_factories(self):
        config = self.config
        self.assertEqual({}, config.portable_factories)

        invalid_configs = [
            {"123": 1},
            {123: "123"},
            {123: {"123": 1}},
            {123: {123: "123"}},
            {123: {123: str}},
            123,
        ]

        for invalid_config in invalid_configs:
            with self.assertRaises(TypeError):
                config.portable_factories = invalid_config

        factories = {1: {2: Portable}}

        config.portable_factories = factories
        self.assertEqual(factories, config.portable_factories)

    def test_class_definitions(self):
        config = self.config
        self.assertEqual([], config.class_definitions)

        with self.assertRaises(TypeError):
            config.class_definitions = [123]

        with self.assertRaises(TypeError):
            config.class_definitions = None

        cds = [ClassDefinition(1, 2, 3)]
        config.class_definitions = cds
        self.assertEqual(cds, config.class_definitions)

    def test_check_class_definition_errors(self):
        config = self.config
        self.assertTrue(config.check_class_definition_errors)

        with self.assertRaises(TypeError):
            config.check_class_definition_errors = None

        config.check_class_definition_errors = False
        self.assertFalse(config.check_class_definition_errors)

    def test_is_big_endian(self):
        config = self.config
        self.assertTrue(config.is_big_endian)

        with self.assertRaises(TypeError):
            config.is_big_endian = None

        config.is_big_endian = False
        self.assertFalse(config.is_big_endian)

    def test_default_int_type(self):
        config = self.config
        self.assertEqual(IntType.INT, config.default_int_type)

        with self.assertRaises(TypeError):
            config.default_int_type = None

        config.default_int_type = IntType.BIG_INT
        self.assertEqual(IntType.BIG_INT, config.default_int_type)

        config.default_int_type = 0
        self.assertEqual(0, config.default_int_type)

        config.default_int_type = "INT"
        self.assertEqual(IntType.INT, config.default_int_type)

    def test_global_serializer(self):
        config = self.config
        self.assertIsNone(config.global_serializer)

        with self.assertRaises(TypeError):
            config.global_serializer = "123"

        with self.assertRaises(TypeError):
            config.global_serializer = str

        config.global_serializer = StreamSerializer
        self.assertEqual(StreamSerializer, config.global_serializer)

    def test_custom_serializers(self):
        config = self.config
        self.assertEqual({}, config.custom_serializers)

        invalid_configs = [
            {1: "123"},
            {str: "123"},
            {str: int},
            None,
        ]

        for invalid_config in invalid_configs:
            with self.assertRaises(TypeError):
                config.custom_serializers = invalid_config

        serializers = {int: StreamSerializer}
        config.custom_serializers = serializers
        self.assertEqual(serializers, config.custom_serializers)

    def test_near_caches_invalid_configs(self):
        config = self.config
        self.assertEqual({}, config.near_caches)

        invalid_configs = [
            ({123: "123"}, TypeError),
            ({"123": 123}, TypeError),
            (None, TypeError),
            ({"x": {"invalidate_on_change": None}}, TypeError),
            ({"x": {"in_memory_format": None}}, TypeError),
            ({"x": {"time_to_live": None}}, TypeError),
            ({"x": {"time_to_live": -1}}, ValueError),
            ({"x": {"max_idle": None}}, TypeError),
            ({"x": {"max_idle": -12}}, ValueError),
            ({"x": {"eviction_policy": None}}, TypeError),
            ({"x": {"eviction_max_size": None}}, TypeError),
            ({"x": {"eviction_max_size": 0}}, ValueError),
            ({"x": {"eviction_sampling_count": None}}, TypeError),
            ({"x": {"eviction_sampling_count": 0}}, ValueError),
            ({"x": {"eviction_sampling_pool_size": None}}, TypeError),
            ({"x": {"eviction_sampling_pool_size": -10}}, ValueError),
            ({"x": {"invalid_option": -10}}, InvalidConfigurationError),
        ]

        for c, e in invalid_configs:
            with self.assertRaises(e):
                config.near_caches = c

    def test_near_caches_defaults(self):
        config = self.config
        config.near_caches = {"a": {}}
        nc_config = config.near_caches["a"]
        self.assertTrue(nc_config.invalidate_on_change)
        self.assertEqual(InMemoryFormat.BINARY, nc_config.in_memory_format)
        self.assertIsNone(nc_config.time_to_live)
        self.assertIsNone(nc_config.max_idle)
        self.assertEqual(EvictionPolicy.LRU, nc_config.eviction_policy)
        self.assertEqual(10000, nc_config.eviction_max_size)
        self.assertEqual(8, nc_config.eviction_sampling_count)
        self.assertEqual(16, nc_config.eviction_sampling_pool_size)

    def test_near_caches_with_a_few_changes(self):
        config = self.config
        config.near_caches = {"a": {"invalidate_on_change": False, "time_to_live": 10}}
        nc_config = config.near_caches["a"]
        self.assertFalse(nc_config.invalidate_on_change)
        self.assertEqual(InMemoryFormat.BINARY, nc_config.in_memory_format)
        self.assertEqual(10, nc_config.time_to_live)
        self.assertIsNone(None, nc_config.max_idle)
        self.assertEqual(EvictionPolicy.LRU, nc_config.eviction_policy)
        self.assertEqual(10000, nc_config.eviction_max_size)
        self.assertEqual(8, nc_config.eviction_sampling_count)
        self.assertEqual(16, nc_config.eviction_sampling_pool_size)

    def test_near_caches(self):
        config = self.config
        config.near_caches = {
            "a": {
                "invalidate_on_change": False,
                "in_memory_format": "OBJECT",
                "time_to_live": 100,
                "max_idle": 200,
                "eviction_policy": "RANDOM",
                "eviction_max_size": 1000,
                "eviction_sampling_count": 20,
                "eviction_sampling_pool_size": 15,
            }
        }
        nc_config = config.near_caches["a"]
        self.assertFalse(nc_config.invalidate_on_change)
        self.assertEqual(InMemoryFormat.OBJECT, nc_config.in_memory_format)
        self.assertEqual(100, nc_config.time_to_live)
        self.assertEqual(200, nc_config.max_idle)
        self.assertEqual(EvictionPolicy.RANDOM, nc_config.eviction_policy)
        self.assertEqual(1000, nc_config.eviction_max_size)
        self.assertEqual(20, nc_config.eviction_sampling_count)
        self.assertEqual(15, nc_config.eviction_sampling_pool_size)

    def test_load_balancer(self):
        config = self.config
        self.assertIsNone(config.load_balancer)

        with self.assertRaises(TypeError):
            config.load_balancer = None

        lb = RandomLB()
        config.load_balancer = lb
        self.assertEqual(lb, config.load_balancer)

    def test_membership_listeners(self):
        config = self.config
        self.assertEqual([], config.membership_listeners)

        with self.assertRaises(TypeError):
            config.membership_listeners = [(None, None)]

        with self.assertRaises(TypeError):
            config.membership_listeners = [None]

        with self.assertRaises(TypeError):
            config.membership_listeners = [(1, 2, 3)]

        with self.assertRaises(TypeError):
            config.membership_listeners = None

        config.membership_listeners = [(None, lambda x: x)]
        added, removed = config.membership_listeners[0]
        self.assertIsNone(added)
        self.assertEqual("x", removed("x"))

        config.membership_listeners = [(lambda x: x, None)]
        added, removed = config.membership_listeners[0]
        self.assertEqual("x", added("x"))
        self.assertIsNone(removed)

        config.membership_listeners = [(lambda x: x, lambda x: x)]
        added, removed = config.membership_listeners[0]
        self.assertEqual("x", added("x"))
        self.assertEqual("x", removed("x"))

    def test_lifecycle_listeners(self):
        config = self.config
        self.assertEqual([], config.lifecycle_listeners)

        with self.assertRaises(TypeError):
            config.lifecycle_listeners = [None]

        with self.assertRaises(TypeError):
            config.lifecycle_listeners = None

        config.lifecycle_listeners = [lambda x: x]
        cb = config.lifecycle_listeners[0]
        self.assertEqual("x", cb("x"))

    def test_flake_id_generators_invalid_configs(self):
        config = self.config
        self.assertEqual({}, config.flake_id_generators)

        invalid_configs = [
            ({123: "123"}, TypeError),
            ({"123": 123}, TypeError),
            (None, TypeError),
            ({"x": {"prefetch_count": None}}, TypeError),
            ({"x": {"prefetch_count": -1}}, ValueError),
            ({"x": {"prefetch_count": 999999}}, ValueError),
            ({"x": {"prefetch_validity": None}}, TypeError),
            ({"x": {"prefetch_validity": -1}}, ValueError),
            ({"x": {"invalid_option": -10}}, InvalidConfigurationError),
        ]

        for c, e in invalid_configs:
            with self.assertRaises(e):
                config.flake_id_generators = c

    def test_flake_id_generators_defaults(self):
        config = self.config
        config.flake_id_generators = {"a": {}}
        fig_config = config.flake_id_generators["a"]
        self.assertEqual(100, fig_config.prefetch_count)
        self.assertEqual(600, fig_config.prefetch_validity)

    def test_flake_id_generators_with_a_few_changes(self):
        config = self.config
        config.flake_id_generators = {"a": {"prefetch_validity": 10}}
        fig_config = config.flake_id_generators["a"]
        self.assertEqual(100, fig_config.prefetch_count)
        self.assertEqual(10, fig_config.prefetch_validity)

    def test_flake_id_generators(self):
        config = self.config
        config.flake_id_generators = {
            "a": {
                "prefetch_count": 20,
                "prefetch_validity": 30,
            }
        }
        fig_config = config.flake_id_generators["a"]
        self.assertEqual(20, fig_config.prefetch_count)
        self.assertEqual(30, fig_config.prefetch_validity)

    def test_labels(self):
        config = self.config
        self.assertEqual([], config.labels)

        with self.assertRaises(TypeError):
            config.labels = ["123", None]

        with self.assertRaises(TypeError):
            config.labels = None

        l = ["123", "345", "qwe"]
        config.labels = l
        self.assertEqual(l, config.labels)

    def test_heartbeat_interval(self):
        config = self.config
        self.assertEqual(5, config.heartbeat_interval)

        with self.assertRaises(ValueError):
            config.heartbeat_interval = -1

        with self.assertRaises(TypeError):
            config.heartbeat_interval = None

        config.heartbeat_interval = 20
        self.assertEqual(20, config.heartbeat_interval)

    def test_heartbeat_timeout(self):
        config = self.config
        self.assertEqual(60, config.heartbeat_timeout)

        with self.assertRaises(ValueError):
            config.heartbeat_timeout = 0

        with self.assertRaises(TypeError):
            config.heartbeat_timeout = None

        config.heartbeat_timeout = 100
        self.assertEqual(100, config.heartbeat_timeout)

    def test_invocation_timeout(self):
        config = self.config
        self.assertEqual(120, config.invocation_timeout)

        with self.assertRaises(ValueError):
            config.invocation_timeout = 0

        with self.assertRaises(TypeError):
            config.invocation_timeout = None

        config.invocation_timeout = 10
        self.assertEqual(10, config.invocation_timeout)

    def test_invocation_retry_pause(self):
        config = self.config
        self.assertEqual(1, config.invocation_retry_pause)

        with self.assertRaises(ValueError):
            config.invocation_retry_pause = -1

        with self.assertRaises(TypeError):
            config.invocation_retry_pause = None

        config.invocation_retry_pause = 11
        self.assertEqual(11, config.invocation_retry_pause)

    def test_statistics_enabled(self):
        config = self.config
        self.assertFalse(config.statistics_enabled)

        with self.assertRaises(TypeError):
            config.statistics_enabled = None

        config.statistics_enabled = True
        self.assertTrue(config.statistics_enabled)

    def test_statistics_period(self):
        config = self.config
        self.assertEqual(3, config.statistics_period)

        with self.assertRaises(ValueError):
            config.statistics_period = -1

        with self.assertRaises(TypeError):
            config.statistics_period = None

        config.statistics_period = 5.5
        self.assertEqual(5.5, config.statistics_period)

    def test_shuffle_member_list(self):
        config = self.config
        self.assertTrue(config.shuffle_member_list)

        with self.assertRaises(TypeError):
            config.shuffle_member_list = None

        config.shuffle_member_list = False
        self.assertFalse(config.shuffle_member_list)

    def test_backup_ack_to_client_enabled(self):
        config = self.config
        self.assertTrue(config.backup_ack_to_client_enabled)

        with self.assertRaises(TypeError):
            config.backup_ack_to_client_enabled = None

        config.backup_ack_to_client_enabled = False
        self.assertFalse(config.backup_ack_to_client_enabled)

    def test_operation_backup_timeout(self):
        config = self.config
        self.assertEqual(5.0, config.operation_backup_timeout)

        with self.assertRaises(ValueError):
            config.operation_backup_timeout = 0

        with self.assertRaises(TypeError):
            config.operation_backup_timeout = None

        config.operation_backup_timeout = 10
        self.assertEqual(10, config.operation_backup_timeout)

    def test_fail_on_indeterminate_operation_state(self):
        config = self.config
        self.assertFalse(config.fail_on_indeterminate_operation_state)

        with self.assertRaises(TypeError):
            config.fail_on_indeterminate_operation_state = None

        config.fail_on_indeterminate_operation_state = True
        self.assertTrue(config.fail_on_indeterminate_operation_state)


class IndexConfigTest(unittest.TestCase):
    def test_defaults(self):
        config = IndexConfig()
        self.assertIsNone(config.name)
        self.assertEqual(IndexType.SORTED, config.type)
        self.assertEqual([], config.attributes)
        self.assertEqual(QueryConstants.KEY_ATTRIBUTE_NAME, config.bitmap_index_options.unique_key)
        self.assertEqual(
            UniqueKeyTransformation.OBJECT, config.bitmap_index_options.unique_key_transformation
        )

    def test_from_dict(self):
        with self.assertRaises(InvalidConfigurationError):
            IndexConfig.from_dict({"unknown_key": 1})

    def test_from_dict_defaults(self):
        config = IndexConfig.from_dict({})
        self.assertIsNone(config.name)
        self.assertEqual(IndexType.SORTED, config.type)
        self.assertEqual([], config.attributes)
        self.assertEqual(QueryConstants.KEY_ATTRIBUTE_NAME, config.bitmap_index_options.unique_key)
        self.assertEqual(
            UniqueKeyTransformation.OBJECT, config.bitmap_index_options.unique_key_transformation
        )

    def test_from_dict_with_changes(self):
        config = IndexConfig.from_dict(
            {
                "name": "test",
            }
        )
        self.assertEqual("test", config.name)
        self.assertEqual(IndexType.SORTED, config.type)
        self.assertEqual([], config.attributes)
        self.assertEqual(QueryConstants.KEY_ATTRIBUTE_NAME, config.bitmap_index_options.unique_key)
        self.assertEqual(
            UniqueKeyTransformation.OBJECT, config.bitmap_index_options.unique_key_transformation
        )

    def test_add_attributes(self):
        config = IndexConfig()

        invalid_attributes = [
            (None, AssertionError),
            ("   ", ValueError),
            ("x.", ValueError),
            ("  x.x.", ValueError),
        ]

        for attr, error in invalid_attributes:
            with self.assertRaises(error):
                config.add_attribute(attr)

        config.add_attribute("x.y")
        config.add_attribute("x.y.z")
        self.assertEqual(["x.y", "x.y.z"], config.attributes)

    def test_with_changes(self):
        name = "name"
        idx_type = IndexType.HASH
        attributes = ["attr", "attr.nested"]
        bio = {
            "unique_key": QueryConstants.THIS_ATTRIBUTE_NAME,
            "unique_key_transformation": UniqueKeyTransformation.RAW,
        }
        config = IndexConfig(name, idx_type, attributes, bio)

        self.assertEqual(name, config.name)
        self.assertEqual(idx_type, config.type)
        self.assertEqual(attributes, attributes)
        self.assertEqual(bio["unique_key"], config.bitmap_index_options.unique_key)
        self.assertEqual(
            bio["unique_key_transformation"], config.bitmap_index_options.unique_key_transformation
        )

    def test_bitmap_index_options(self):
        config = IndexConfig()

        config.bitmap_index_options = {"unique_key": QueryConstants.THIS_ATTRIBUTE_NAME}
        self.assertEqual(QueryConstants.THIS_ATTRIBUTE_NAME, config.bitmap_index_options.unique_key)
        self.assertEqual(
            UniqueKeyTransformation.OBJECT, config.bitmap_index_options.unique_key_transformation
        )

        invalid_options = [
            ({"unique_key": None}, TypeError),
            ({"unique_key_transformation": None}, TypeError),
            ({"invalid_config": None}, InvalidConfigurationError),
            ([], TypeError),
        ]

        for o, e in invalid_options:
            with self.assertRaises(e):
                config.bitmap_index_options = o

    def test_name(self):
        config = IndexConfig()

        config.name = "test"
        self.assertEqual("test", config.name)

        with self.assertRaises(TypeError):
            config.name = 123

    def test_type(self):
        config = IndexConfig()

        config.type = IndexType.BITMAP
        self.assertEqual(IndexType.BITMAP, config.type)

        config.type = "HASH"
        self.assertEqual(IndexType.HASH, config.type)

        with self.assertRaises(TypeError):
            config.type = "HASHH"

    def test_attributes(self):
        config = IndexConfig()

        config.attributes = ["a"]
        self.assertEqual(["a"], config.attributes)

        with self.assertRaises(TypeError):
            config.attributes = None

        with self.assertRaises(ValueError):
            config.attributes = ["a."]


class BitmapIndexOptionsTest(unittest.TestCase):
    def test_defaults(self):
        options = BitmapIndexOptions()
        self.assertEqual(QueryConstants.KEY_ATTRIBUTE_NAME, options.unique_key)
        self.assertEqual(UniqueKeyTransformation.OBJECT, options.unique_key_transformation)

    def test_from_dict(self):
        with self.assertRaises(InvalidConfigurationError):
            BitmapIndexOptions.from_dict({"unknown_key": 1})

    def test_from_dict_defaults(self):
        options = BitmapIndexOptions.from_dict({})
        self.assertEqual(QueryConstants.KEY_ATTRIBUTE_NAME, options.unique_key)
        self.assertEqual(UniqueKeyTransformation.OBJECT, options.unique_key_transformation)

    def test_from_dict_with_changes(self):
        options = BitmapIndexOptions.from_dict(
            {
                "unique_key": QueryConstants.THIS_ATTRIBUTE_NAME,
            }
        )
        self.assertEqual(QueryConstants.THIS_ATTRIBUTE_NAME, options.unique_key)
        self.assertEqual(UniqueKeyTransformation.OBJECT, options.unique_key_transformation)

    def test_unique_key(self):
        options = BitmapIndexOptions()

        options.unique_key = QueryConstants.THIS_ATTRIBUTE_NAME
        self.assertEqual(options.unique_key, QueryConstants.THIS_ATTRIBUTE_NAME)

        options.unique_key = "KEY_ATTRIBUTE_NAME"
        self.assertEqual(options.unique_key, QueryConstants.KEY_ATTRIBUTE_NAME)

        with self.assertRaises(TypeError):
            options.unique_key = None

    def test_unique_key_transformation(self):
        options = BitmapIndexOptions()

        options.unique_key_transformation = UniqueKeyTransformation.RAW
        self.assertEqual(options.unique_key_transformation, UniqueKeyTransformation.RAW)

        options.unique_key_transformation = "LONG"
        self.assertEqual(options.unique_key_transformation, UniqueKeyTransformation.LONG)

        with self.assertRaises(TypeError):
            options.unique_key_transformation = 132
