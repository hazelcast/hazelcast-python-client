""" Configuration module """

DEFAULT_GROUP_NAME = "dev"
DEFAULT_GROUP_PASSWORD = "dev-pass"


class ClientConfig(object):
    def __init__(self):
        self.properties = {}
        self.group_config = GroupConfig()
        self.network_config = ClientNetworkConfig()
        self.load_balancer = None
        self.listener_configs = []
        self.executor_pool_size = -1
        self.instance_name = None
        self.near_cache_configs = {}
        self.reliable_topic_configs = {}
        self.query_cache_configs = {}
        self.serialization_config = SerializationConfig()
        # self.proxy_factory_configs = {}


class GroupConfig(object):
    def __init__(self):
        self.name = DEFAULT_GROUP_NAME
        self.password = DEFAULT_GROUP_PASSWORD


class ClientNetworkConfig(object):
    def __init__(self):
        self.addresses = []
        self.connection_attempt_limit = 2
        self.connection_attempt_period = 3
        self.connection_timeout = 5
        self.redo_operation = False
        self.smart_routing = True
        self.socket_options = SocketOptions()


class SocketOptions(object):
    def __init__(self):
        pass


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
