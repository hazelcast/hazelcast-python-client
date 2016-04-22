""" Configuration module """

from hazelcast.serialization.api import StreamSerializer
from hazelcast.util import validate_type, validate_serializer, enum

DEFAULT_GROUP_NAME = "dev"
DEFAULT_GROUP_PASSWORD = "dev-pass"

PROPERTY_HEARTBEAT_INTERVAL = "hazelcast.client.heartbeat.interval"
PROPERTY_HEARTBEAT_TIMEOUT = "hazelcast.client.heartbeat.timeout"

INTEGER_TYPE = enum(VAR=0, BYTE=1, SHORT=2, INT=3, LONG=4, BIG_INT=5)

EVICTION_POLICY = enum(NONE=0, LRU=1, LFU=2, RANDOM=3)
IN_MEMORY_FORMAT = enum(BINARY=0, OBJECT=1)

DEFAULT_MAX_ENTRY_COUNT = 10000
DEFAULT_SAMPLING_COUNT = 8
DEFAULT_SAMPLING_POOL_SIZE = 16


class ClientConfig(object):
    def __init__(self):
        self.properties = {}
        self.group_config = GroupConfig()
        self.network_config = ClientNetworkConfig()
        self.load_balancer = None
        self.membership_listeners = []
        self.lifecycle_listeners = []
        self.near_cache_configs = {}  # map_name:near_cache_config
        self.serialization_config = SerializationConfig()

    def get_property_or_default(self, key, default):
        try:
            return self.properties[key]
        except KeyError:
            return default

    def add_near_cache_config(self, near_cache_config):
        self.near_cache_configs[near_cache_config.name] = near_cache_config


class GroupConfig(object):
    def __init__(self):
        self.name = DEFAULT_GROUP_NAME
        self.password = DEFAULT_GROUP_PASSWORD


class ClientNetworkConfig(object):
    def __init__(self):
        self.addresses = []
        self.connection_attempt_limit = 4
        self.connection_attempt_period = 3
        # self.connection_timeout = 5  TODO
        # self.socket_options = None TODO
        self.redo_operation = False
        self.smart_routing = True


class SerializationConfig(object):
    def __init__(self):
        self.portable_version = 0
        self.data_serializable_factories = {}
        self.portable_factories = {}
        self.class_definitions = set()
        self.global_serializer = None
        self.custom_serializers = {}
        self.check_class_def_errors = True
        self.is_big_endian = True
        self.default_integer_type = INTEGER_TYPE.INT

    def add_portable_factory(self, factory_id, factory):
        self.portable_factories[factory_id] = factory

    def add_data_serializable_factory(self, factory_id, factory):
        self.data_serializable_factories[factory_id] = factory

    def set_custom_serializer(self, _type, serializer):
        validate_type(_type)
        validate_serializer(serializer, StreamSerializer)
        self.custom_serializers[_type] = serializer

    def set_global_serializer(self, global_serializer):
        validate_serializer(global_serializer, StreamSerializer)
        self.global_serializer = global_serializer


class NearCacheConfig(object):
    def __init__(self, name=None):
        self.name = name
        self._in_memory_format = IN_MEMORY_FORMAT.BINARY
        self._time_to_live_seconds = None
        self._max_idle_seconds = None
        self.invalidate_on_change = True
        self._eviction_policy = EVICTION_POLICY.NONE
        self._eviction_max_size = DEFAULT_MAX_ENTRY_COUNT
        self._eviction_sampling_count = DEFAULT_SAMPLING_COUNT
        self._eviction_sampling_pool_size = DEFAULT_SAMPLING_POOL_SIZE

    @property
    def in_memory_format(self):
        return self._in_memory_format

    @in_memory_format.setter
    def in_memory_format(self, in_memory_format):
        if in_memory_format not in IN_MEMORY_FORMAT.reverse:
            raise ValueError("Invalid in-memory-format :{}".format(in_memory_format))
        self._in_memory_format = in_memory_format

    @property
    def time_to_live_seconds(self):
        return self._time_to_live_seconds

    @time_to_live_seconds.setter
    def time_to_live_seconds(self, time_to_live_seconds):
        if time_to_live_seconds < 0:
            raise ValueError("'time_to_live_seconds' cannot be less than 0")
        self._time_to_live_seconds = time_to_live_seconds

    @property
    def max_idle_seconds(self):
        return self._max_idle_seconds

    @max_idle_seconds.setter
    def max_idle_seconds(self, max_idle_seconds):
        if max_idle_seconds < 0:
            raise ValueError("'max_idle_seconds' cannot be less than 0")
        self._max_idle_seconds = max_idle_seconds

    @property
    def eviction_policy(self):
        return self._eviction_policy

    @eviction_policy.setter
    def eviction_policy(self, eviction_policy):
        if eviction_policy not in EVICTION_POLICY.reverse:
            raise ValueError("Invalid eviction_policy :{}".format(eviction_policy))
        self._eviction_policy = eviction_policy

    @property
    def eviction_max_size(self):
        return self._eviction_max_size

    @eviction_max_size.setter
    def eviction_max_size(self, eviction_max_size):
        if eviction_max_size < 1:
            raise ValueError("'Eviction-max-size' cannot be less than 1")
        self._eviction_max_size = eviction_max_size

    @property
    def eviction_sampling_count(self):
        return self._eviction_sampling_count

    @eviction_sampling_count.setter
    def eviction_sampling_count(self, eviction_sampling_count):
        if eviction_sampling_count < 1:
            raise ValueError("'eviction_sampling_count' cannot be less than 1")
        self._eviction_sampling_count = eviction_sampling_count

    @property
    def eviction_sampling_pool_size(self):
        return self._eviction_sampling_pool_size

    @eviction_sampling_pool_size.setter
    def eviction_sampling_pool_size(self, eviction_sampling_pool_size):
        if eviction_sampling_pool_size < 1:
            raise ValueError("'eviction_sampling_pool_size' cannot be less than 1")
        self._eviction_sampling_pool_size = eviction_sampling_pool_size
