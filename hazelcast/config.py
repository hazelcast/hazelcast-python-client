""" Configuration module """

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
        self.data_serializable_factory_classes = {}
        self.data_serializable_factories = {}
        self.portable_factory_classes = {}
        self.portable_factories = {}
        self.global_serializer_config = None
        self.serializer_configs = []
        self.check_class_def_errors = True
        self.use_native_byte_order = False
        self.is_big_endian = True
        self.enable_compression = False
        self.enable_shared_object = True
        self.class_definitions = set()


class GlobalSerializerConfig(object):
    def __init__(self):
        pass
