import socket
import typing
import unittest

from hazelcast.config import (
    Config,
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
    NearCacheConfig,
    FlakeIdGeneratorConfig,
    ReliableTopicConfig,
)
from hazelcast.core import Address
from hazelcast.errors import InvalidConfigurationError
from hazelcast.security import BasicTokenProvider, TokenProvider
from hazelcast.serialization.api import (
    IdentifiedDataSerializable,
    Portable,
    StreamSerializer,
    ObjectDataInput,
    ObjectDataOutput,
    PortableWriter,
    PortableReader,
    CompactSerializer,
    CompactSerializableClass,
    CompactWriter,
    CompactReader,
)
from hazelcast.serialization.portable.classdef import ClassDefinition, ClassDefinitionBuilder
from hazelcast.util import RandomLB


class ConfigTest(unittest.TestCase):
    def setUp(self):
        self.config = Config()

    def test_from_dict(self):
        config_dict = {
            "cluster_members": ["192.168.1.1:5704"],
            "cluster_name": "not-dev",
            "client_name": "client0",
            "connection_timeout": 1.0,
            "socket_options": [(socket.IPPROTO_IP, socket.IP_HDRINCL, 1)],
            "redo_operation": True,
            "smart_routing": False,
            "ssl_enabled": True,
            "ssl_cafile": "ca.pem",
            "ssl_certfile": "cert.pem",
            "ssl_keyfile": "key.pem",
            "ssl_password": "pass",
            "ssl_protocol": SSLProtocol.TLSv1_3,
            "ssl_ciphers": "DHE-RSA-AES128-SHA",
            "cloud_discovery_token": "abc123",
            "async_start": True,
            "reconnect_mode": ReconnectMode.OFF,
            "retry_initial_backoff": 9,
            "retry_max_backoff": 99,
            "retry_jitter": 0.9,
            "retry_multiplier": 1.9,
            "cluster_connect_timeout": 999,
            "portable_version": 10,
            "data_serializable_factories": {1: {1: SomeIdentified}},
            "portable_factories": {2: {2: SomePortable}},
            "compact_serializers": [SomeClassSerializer()],
            "class_definitions": [CLASS_DEFINITION],
            "check_class_definition_errors": False,
            "is_big_endian": False,
            "default_int_type": IntType.LONG,
            "global_serializer": GlobalSerializer,
            "custom_serializers": {CustomSerializable: CustomSerializer},
            "near_caches": {
                "map0": {
                    "invalidate_on_change": False,
                    "in_memory_format": InMemoryFormat.OBJECT,
                    "time_to_live": 1,
                    "max_idle": 2,
                    "eviction_policy": EvictionPolicy.RANDOM,
                    "eviction_max_size": 999,
                    "eviction_sampling_count": 9,
                    "eviction_sampling_pool_size": 99,
                }
            },
            "load_balancer": RandomLB(),
            "membership_listeners": [(lambda m: print(m), None)],
            "lifecycle_listeners": [lambda s: print(s)],
            "flake_id_generators": {
                "gen0": {
                    "prefetch_count": 99,
                    "prefetch_validity": 999,
                }
            },
            "reliable_topics": {
                "topic0": {
                    "read_batch_size": 9,
                    "overload_policy": TopicOverloadPolicy.ERROR,
                }
            },
            "labels": ["label"],
            "heartbeat_interval": 9,
            "heartbeat_timeout": 99,
            "invocation_timeout": 999,
            "invocation_retry_pause": 9.5,
            "statistics_enabled": True,
            "statistics_period": 9,
            "shuffle_member_list": False,
            "backup_ack_to_client_enabled": False,
            "operation_backup_timeout": 99.9,
            "fail_on_indeterminate_operation_state": True,
            "creds_username": "user",
            "creds_password": "pass",
            "token_provider": SomeTokenProvider(),
            "use_public_ip": True,
        }

        config = Config.from_dict(config_dict)
        self.assertEqual(["192.168.1.1:5704"], config.cluster_members)
        self.assertEqual("not-dev", config.cluster_name)
        self.assertEqual("client0", config.client_name)
        self.assertEqual(1.0, config.connection_timeout)
        self.assertEqual([(socket.IPPROTO_IP, socket.IP_HDRINCL, 1)], config.socket_options)
        self.assertTrue(config.redo_operation)
        self.assertFalse(config.smart_routing)
        self.assertTrue(config.ssl_enabled)
        self.assertEqual("ca.pem", config.ssl_cafile)
        self.assertEqual("cert.pem", config.ssl_certfile)
        self.assertEqual("key.pem", config.ssl_keyfile)
        self.assertEqual("pass", config.ssl_password)
        self.assertEqual(SSLProtocol.TLSv1_3, config.ssl_protocol)
        self.assertEqual("DHE-RSA-AES128-SHA", config.ssl_ciphers)
        self.assertEqual("abc123", config.cloud_discovery_token)
        self.assertTrue(config.async_start)
        self.assertEqual(ReconnectMode.OFF, config.reconnect_mode)
        self.assertEqual(9, config.retry_initial_backoff)
        self.assertEqual(99, config.retry_max_backoff)
        self.assertEqual(0.9, config.retry_jitter)
        self.assertEqual(1.9, config.retry_multiplier)
        self.assertEqual(999, config.cluster_connect_timeout)
        self.assertEqual(10, config.portable_version)
        self.assertEqual({1: {1: SomeIdentified}}, config.data_serializable_factories)
        self.assertEqual({2: {2: SomePortable}}, config.portable_factories)
        self.assertEqual(1, len(config.compact_serializers))
        self.assertIsInstance(config.compact_serializers[0], SomeClassSerializer)
        self.assertEqual([CLASS_DEFINITION], config.class_definitions)
        self.assertFalse(config.check_class_definition_errors)
        self.assertFalse(config.is_big_endian)
        self.assertEqual(IntType.LONG, config.default_int_type)
        self.assertEqual(GlobalSerializer, config.global_serializer)
        self.assertEqual({CustomSerializable: CustomSerializer}, config.custom_serializers)

        nc_config = config.near_caches["map0"]
        self.assertFalse(nc_config.invalidate_on_change)
        self.assertEqual(InMemoryFormat.OBJECT, nc_config.in_memory_format)
        self.assertEqual(1, nc_config.time_to_live)
        self.assertEqual(2, nc_config.max_idle)
        self.assertEqual(EvictionPolicy.RANDOM, nc_config.eviction_policy)
        self.assertEqual(999, nc_config.eviction_max_size)
        self.assertEqual(9, nc_config.eviction_sampling_count)
        self.assertEqual(99, nc_config.eviction_sampling_pool_size)

        self.assertIsInstance(config.load_balancer, RandomLB)

        membership_listeners = config.membership_listeners
        self.assertEqual(1, len(membership_listeners))
        self.assertTrue(callable(membership_listeners[0][0]))
        self.assertIsNone(membership_listeners[0][1])

        lifecycle_listeners = config.lifecycle_listeners
        self.assertEqual(1, len(lifecycle_listeners))
        self.assertTrue(callable(lifecycle_listeners[0]))

        fig_config = config.flake_id_generators["gen0"]
        self.assertEqual(99, fig_config.prefetch_count)
        self.assertEqual(999, fig_config.prefetch_validity)

        reliable_topic_config = config.reliable_topics["topic0"]
        self.assertEqual(9, reliable_topic_config.read_batch_size)
        self.assertEqual(TopicOverloadPolicy.ERROR, reliable_topic_config.overload_policy)

        self.assertEqual(["label"], config.labels)
        self.assertEqual(9, config.heartbeat_interval)
        self.assertEqual(99, config.heartbeat_timeout)
        self.assertEqual(999, config.invocation_timeout)
        self.assertEqual(9.5, config.invocation_retry_pause)
        self.assertTrue(config.statistics_enabled)
        self.assertEqual(9, config.statistics_period)
        self.assertFalse(config.shuffle_member_list)
        self.assertFalse(config.backup_ack_to_client_enabled)
        self.assertEqual(99.9, config.operation_backup_timeout)
        self.assertTrue(config.fail_on_indeterminate_operation_state)
        self.assertEqual("user", config.creds_username)
        self.assertEqual("pass", config.creds_password)
        self.assertIsInstance(config.token_provider, SomeTokenProvider)
        self.assertTrue(config.use_public_ip)

    def test_from_dict_defaults(self):
        config = Config.from_dict({})
        for item in self.config.__slots__:
            self.assertEqual(getattr(self.config, item), getattr(config, item))

    def test_from_dict_with_a_few_changes(self):
        config = Config.from_dict({"client_name": "hazel", "cluster_name": "cast"})
        for item in self.config.__slots__:
            if item == "_client_name" or item == "_cluster_name":
                continue
            self.assertEqual(getattr(self.config, item), getattr(config, item))

        self.assertEqual("hazel", config.client_name)
        self.assertEqual("cast", config.cluster_name)

    def test_from_dict_skip_none_item(self):
        config = Config.from_dict({"cluster_name": None, "cluster_members": None})
        for item in self.config.__slots__:
            self.assertEqual(getattr(self.config, item), getattr(config, item))

    def test_from_dict_with_invalid_elements(self):
        with self.assertRaises(InvalidConfigurationError):
            Config.from_dict({"invalid_elem": False})

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

        options = [(1, 2, 3), (4, 5, 6)]
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

        config.ssl_password = b"123"
        self.assertEqual(b"123", config.ssl_password)

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

    def test_ssl_check_hostname(self):
        config = self.config
        self.assertFalse(config.ssl_check_hostname)

        with self.assertRaises(TypeError):
            config.ssl_check_hostname = "localhost"

        config.ssl_check_hostname = True
        self.assertTrue(config.ssl_check_hostname)

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

    def test_compact_serializers(self):
        config = self.config
        self.assertEqual([], config.compact_serializers)

        invalid_configs = [
            [None],
            [""],
            [SomeClassSerializer(), 1],
            [SomeClassSerializer(), None],
            "",
        ]

        for invalid_config in invalid_configs:
            with self.assertRaises(TypeError):
                config.compact_serializers = invalid_config

        serializers = [SomeClassSerializer()]
        config.compact_serializers = serializers

        self.assertEqual(serializers, config.compact_serializers)

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

    def test_near_cache_config_from_dict(self):
        nc_config_dict = {
            "invalidate_on_change": False,
            "in_memory_format": "OBJECT",
            "time_to_live": 10,
            "max_idle": 20,
            "eviction_policy": EvictionPolicy.NONE,
            "eviction_max_size": 9999,
            "eviction_sampling_count": 99,
            "eviction_sampling_pool_size": 999,
        }

        nc_config = NearCacheConfig.from_dict(nc_config_dict)
        self.assertFalse(nc_config.invalidate_on_change)
        self.assertEqual(InMemoryFormat.OBJECT, nc_config.in_memory_format)
        self.assertEqual(10, nc_config.time_to_live)
        self.assertEqual(20, nc_config.max_idle)
        self.assertEqual(EvictionPolicy.NONE, nc_config.eviction_policy)
        self.assertEqual(9999, nc_config.eviction_max_size)
        self.assertEqual(99, nc_config.eviction_sampling_count)
        self.assertEqual(999, nc_config.eviction_sampling_pool_size)

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

    def test_flake_id_generator_config_from_dict(self):
        fig_config_dict = {"prefetch_count": 999, "prefetch_validity": 9999.99}

        fig_config = FlakeIdGeneratorConfig.from_dict(fig_config_dict)
        self.assertEqual(999, fig_config.prefetch_count)
        self.assertEqual(9999.99, fig_config.prefetch_validity)

    def test_reliable_topics_invalid_configs(self):
        config = self.config
        self.assertEqual({}, config.reliable_topics)

        invalid_configs = [
            ({123: "123"}, TypeError),
            ({"123": 123}, TypeError),
            (None, TypeError),
            ({"x": {"overload_policy": None}}, TypeError),
            ({"x": {"overload_policy": -1}}, TypeError),
            ({"x": {"read_batch_size": None}}, TypeError),
            ({"x": {"read_batch_size": -1}}, ValueError),
            ({"x": {"read_batch_size": 0}}, ValueError),
            ({"x": {"invalid_option": -10}}, InvalidConfigurationError),
        ]

        for c, e in invalid_configs:
            with self.assertRaises(e):
                config.reliable_topics = c

    def test_reliable_topics_defaults(self):
        config = self.config
        config.reliable_topics = {"a": {}}
        topic_config = config.reliable_topics["a"]
        self.assertEqual(TopicOverloadPolicy.BLOCK, topic_config.overload_policy)
        self.assertEqual(10, topic_config.read_batch_size)

    def test_reliable_topics_with_a_few_changes(self):
        config = self.config
        config.reliable_topics = {
            "a": {
                "read_batch_size": 42,
            }
        }
        topic_config = config.reliable_topics["a"]
        self.assertEqual(TopicOverloadPolicy.BLOCK, topic_config.overload_policy)
        self.assertEqual(42, topic_config.read_batch_size)

    def test_reliable_topics(self):
        config = self.config
        config.reliable_topics = {
            "a": {
                "overload_policy": TopicOverloadPolicy.ERROR,
                "read_batch_size": 42,
            }
        }

        topic_config = config.reliable_topics["a"]
        self.assertEqual(TopicOverloadPolicy.ERROR, topic_config.overload_policy)
        self.assertEqual(42, topic_config.read_batch_size)

    def test_reliable_topic_config_from_dict(self):
        rt_config_dict = {
            "overload_policy": TopicOverloadPolicy.DISCARD_NEWEST,
            "read_batch_size": 84,
        }

        rt_config = ReliableTopicConfig.from_dict(rt_config_dict)
        self.assertEqual(TopicOverloadPolicy.DISCARD_NEWEST, rt_config.overload_policy)
        self.assertEqual(84, rt_config.read_batch_size)

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

    def test_auth_fromdict(self):
        tp = BasicTokenProvider("tok")
        cfg = Config().from_dict(
            {
                "creds_username": "user",
                "creds_password": "pass",
                "token_provider": tp,
            }
        )
        self.assertEqual("user", cfg.creds_username)
        self.assertEqual("pass", cfg.creds_password)
        self.assertEqual(tp, cfg.token_provider)

    def test_auth_failure(self):
        cfg = Config()
        with self.assertRaises(TypeError):
            cfg.creds_username = 1
        with self.assertRaises(TypeError):
            cfg.creds_password = 2
        with self.assertRaises(TypeError):
            cfg.token_provider = object()

    def test_use_public_ip(self):
        config = self.config
        self.assertFalse(config.use_public_ip)

        with self.assertRaises(TypeError):
            config.use_public_ip = None

        config.use_public_ip = True
        self.assertTrue(config.use_public_ip)


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


CLASS_DEFINITION = ClassDefinitionBuilder(1, 1).add_int_field("a").build()


class SomeIdentified(IdentifiedDataSerializable):
    def write_data(self, object_data_output: ObjectDataOutput) -> None:
        pass

    def read_data(self, object_data_input: ObjectDataInput) -> None:
        pass

    def get_factory_id(self) -> int:
        return 1

    def get_class_id(self) -> int:
        return 1


class SomePortable(Portable):
    def write_portable(self, writer: "PortableWriter") -> None:
        pass

    def read_portable(self, reader: "PortableReader") -> None:
        pass

    def get_factory_id(self) -> int:
        return 2

    def get_class_id(self) -> int:
        return 2


class GlobalSerializer(StreamSerializer):
    def write(self, out: ObjectDataOutput, obj: typing.Any) -> None:
        pass

    def read(self, inp: ObjectDataInput) -> typing.Any:
        pass

    def get_type_id(self) -> int:
        return 1

    def destroy(self) -> None:
        pass


class CustomSerializable:
    pass


class CustomSerializer(StreamSerializer):
    def write(self, out: ObjectDataOutput, obj: typing.Any) -> None:
        pass

    def read(self, inp: ObjectDataInput) -> typing.Any:
        pass

    def get_type_id(self) -> int:
        return 2

    def destroy(self) -> None:
        pass


class SomeTokenProvider(TokenProvider):
    def token(self, address: Address = None) -> bytes:
        return b""


class SomeClass:
    pass


class SomeClassSerializer(CompactSerializer[SomeClass]):
    def read(self, reader: CompactReader) -> SomeClass:
        return SomeClass()

    def write(self, writer: CompactWriter, obj: SomeClass) -> None:
        pass

    def get_class(self) -> typing.Type[SomeClass]:
        return SomeClass

    def get_type_name(self) -> str:
        return "SomeClass"
