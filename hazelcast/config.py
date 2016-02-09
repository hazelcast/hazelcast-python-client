""" Configuration module """
from types import TypeType

from hazelcast.serialization.api import StreamSerializer

DEFAULT_GROUP_NAME = "dev"
DEFAULT_GROUP_PASSWORD = "dev-pass"

PROPERTY_HEARTBEAT_INTERVAL = "hazelcast.client.heartbeat.interval"
PROPERTY_HEARTBEAT_TIMEOUT = "hazelcast.client.heartbeat.timeout"


class ClientConfig(object):
    def __init__(self):
        self.properties = {}
        self.group_config = GroupConfig()
        self.network_config = ClientNetworkConfig()
        self.load_balancer = None
        self.membership_listeners = []
        self.lifecycle_listeners = []
        # self.near_cache_configs = {} TODO
        # self.reliable_topic_configs = {} TODO
        self.serialization_config = SerializationConfig()

    def get_property_or_default(self, key, default):
        try:
            return self.properties[key]
        except KeyError:
            return default


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
        self.global_serializer_config = None
        self.serializer_configs = []
        self.check_class_def_errors = True
        self.use_native_byte_order = False
        self.is_big_endian = True
        self.enable_compression = False
        self.enable_shared_object = True
        self.class_definitions = set()

    def add_data_serializable_factory(self, factory):
        self.data_serializable_factories[factory.factory_id] = factory


class BaseSerializerConfig(object):
    def __init__(self, serializer=None):
        self._serializer = serializer

    @property
    def serializer(self):
        return self._serializer

    @serializer.setter
    def serializer(self, value):
        if isinstance(value, StreamSerializer):
            self._serializer = value
        else:
            raise ValueError("Serializer should be an instance of 'hazelcast.serialization.api.StreamSerializer'")


class GlobalSerializerConfig(BaseSerializerConfig):
    def __init__(self, serializer=None):
        super(GlobalSerializerConfig, self).__init__(serializer)


class SerializerConfig(BaseSerializerConfig):
    def __init__(self, serializer=None, object_type=None):
        super(SerializerConfig, self).__init__(serializer)
        self._type = object_type

    @property
    def type(self):
        return self._type

    @type.setter
    def set_type(self, _type):
        if isinstance(type, TypeType):
            self._type = _type
        else:
            raise ValueError("Provided value is not a type")
