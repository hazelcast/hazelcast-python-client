"""
Hazelcast Client Configuration module contains configuration classes and various constants required to create a ClientConfig.

"""

from hazelcast.serialization.api import StreamSerializer
from hazelcast.util import validate_type, validate_serializer, enum

DEFAULT_GROUP_NAME = "dev"
"""
Default group name of the connected Hazelcast cluster
"""

DEFAULT_GROUP_PASSWORD = "dev-pass"
"""
Default password of connected Hazelcast cluster
"""

PROPERTY_HEARTBEAT_INTERVAL = "hazelcast.client.heartbeat.interval"
"""
Configuration property for heartbeat interval in milliseconds. Client will send heartbeat to server by this value of interval
unless other requests send to server
"""

PROPERTY_HEARTBEAT_TIMEOUT = "hazelcast.client.heartbeat.timeout"
"""
Configuration property for heartbeat timeout in milliseconds. If client cannot see any activity on a connection for this timeout
value it will shutdown the connection
"""

INTEGER_TYPE = enum(VAR=0, BYTE=1, SHORT=2, INT=3, LONG=4, BIG_INT=5)
"""
Integer type options that can be used by serialization service.

* VAR : variable size integer (this option can be problematic on static type clients like java or .NET)
* BYTE: Python int will be interpreted as a single byte int
* SHORT: Python int will be interpreted as a double byte int
* INT: Python int will be interpreted as a four byte int
* LONG: Python int will be interpreted as an eight byte int
* BIG_INT: Python int will be interpreted as Java BigInteger. This option can handle python long values with "bit_length > 64"

"""

EVICTION_POLICY = enum(NONE=0, LRU=1, LFU=2, RANDOM=3)
"""
Near Cache eviction policy options

* NONE : No evcition
* LRU : Least Recently Used items will be evicted
* LFU : Least frequently Used items will be evicted
* RANDOM : Items will be evicted randomly

"""

IN_MEMORY_FORMAT = enum(BINARY=0, OBJECT=1)
"""
Near Cache in memory format of the values.

* BINARY : Binary format, hazelcast serializated bytearray format
* OBJECT : The actual objects used

"""

DEFAULT_MAX_ENTRY_COUNT = 10000
DEFAULT_SAMPLING_COUNT = 8
DEFAULT_SAMPLING_POOL_SIZE = 16


class ClientConfig(object):
    """
    The root configuration for hazelcast python client.

        client_config = ClientConfig()
        client = HazelcastClient(client_config)
    """

    def __init__(self):
        self._properties = {}

        self.group_config = GroupConfig()
        """The group configuration"""

        self.network_config = ClientNetworkConfig()
        """The network configuration for addresses to connect, smart-routing, socket-options..."""

        self.load_balancer = None
        """Custom load balancer used to distribute the operations to multiple Endpoints."""

        self.membership_listeners = []
        """Membership listeners, an array of tuple (member_added, member_removed, fire_for_existing)"""

        self.lifecycle_listeners = []
        """ Lifecycle Listeners, an array of Functions of f(state)"""

        self.near_cache_configs = {}  # map_name:NearCacheConfig
        """Near Cache configuration which maps "map-name : NearCacheConfig"""

        self.serialization_config = SerializationConfig()
        """Hazelcast serialization configuration"""

    def add_membership_listener(self, member_added=None, member_removed=None, fire_for_existing=False):
        """
        Helper method for adding membership listeners

        :param member_added: Function that will be called in case of a member added. In the form of f(member)
        :param member_removed: Function that will be called in case of a member removed. In the form of f(member)
        :param fire_for_existing: if True, already existing members will fire member_added event
        :return: `self` for cascading configuration
        """
        self.membership_listeners.append((member_added, member_removed, fire_for_existing))
        return self

    def add_lifecycle_listener(self, lifecycle_state_changed=None):
        """
        Helper method for adding lifecycle listeners

        :param lifecycle_state_changed: Function that will be called in case of the lifecycle state changed.
        In the form of f(state)
        :return: `self` for cascading configuration
        """
        if lifecycle_state_changed:
            self.lifecycle_listeners.append(lifecycle_state_changed)
        return self

    def get_property_or_default(self, key, default):
        """
        client property accessor with fallback to default value

        :param key: property key to access
        :param default: the default value for fallback
        :return: property value if it exist or the default value otherwise
        """
        try:
            return self._properties[key]
        except KeyError:
            return default

    def add_near_cache_config(self, near_cache_config):
        """
        Helper method to add a new NearCacheConfig

        :param near_cache_config: the near_cache config to add
        :return: `self` for cascading configuration
        """
        self.near_cache_configs[near_cache_config.name] = near_cache_config
        return self


class GroupConfig(object):
    """
    The Group Configuration is the container class for name and password of the cluster
    """

    def __init__(self):
        self.name = DEFAULT_GROUP_NAME
        """The group name of the cluster"""
        self.password = DEFAULT_GROUP_PASSWORD
        """The password of the cluster"""


class ClientNetworkConfig(object):
    """
    Network related configuration parameters
    """

    def __init__(self):
        self.addresses = []
        """The candidate address list that client will use to establish initial connection"""
        self.connection_attempt_limit = 2
        """
        While client is trying to connect initially to one of the members in the addressList, all might be not available.
        Instead of giving up, throwing Error and stopping client, it will attempt to retry as much as defined by this parameter.
        """
        self.connection_attempt_period = 3
        """Period for the next attempt to find a member to connect"""
        self.connection_timeout = 5.0
        """
        Socket connection timeout is a float, giving in seconds, or None.
        Setting a timeout of None disables the timeout feature and is equivalent to block the socket until it connects.
        Setting a timeout of zero is the same as disables blocking on connect.
        """
        self.socket_options = []
        """
        Array of Unix socket options.

        Example usage:

            >>> import socket
            >>> client_network_config.socket_options.append(SocketOption(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1))
            >>> client_network_config.socket_options.append(SocketOption(socket.SOL_SOCKET, socket.SO_SNDBUF, 32768))
            >>> client_network_config.socket_options.append(SocketOption(socket.SOL_SOCKET, socket.SO_RCVBUF, 32768))

        Please see the Unix manual for level and option. Level and option constant are in python std lib socket module
        """
        self.redo_operation = False
        """
        If true, client will redo the operations that were executing on the server and client lost the connection.
        This can be because of network, or simply because the member died. However it is not clear whether the application is
        performed or not. For idempotent operations this is harmless, but for non idempotent ones retrying can cause to
        undesirable effects. Note that the redo can perform on any member.
        """
        self.smart_routing = True
        """
        If true, client will route the key based operations to owner of the key at the best effort. Note that it uses a cached
        value of partition count and doesn't guarantee that the operation will always be executed on the owner.
        The cached table is updated every 10 seconds.
        """


class SocketOption(object):
    """
    Advanced configuration for fine-tune the TCP options.
    A Socket option represent the unix socket option, that will be passed to python socket.setoption(level,`option, value)`
    See the Unix manual for level and option.
    """
    def __init__(self, level, option, value):
        self.level = level
        """Option level. See the Unix manual for detail."""
        self.option = option
        """The actual socket option. The actual socket option."""
        self.value = value
        """Socket option value. The value argument can either be an integer or a string"""


class SerializationConfig(object):
    """
    Hazelcast Serialization Service configuration options can be set from this class.
    """
    def __init__(self):
        self.portable_version = 0
        """
        Portable version will be used to differentiate two versions of the same class that have changes on the class,
        like adding/removing a field or changing a type of a field.
        """
        self.data_serializable_factories = {}
        """
        Dictionary of factory-id and corresponding IdentifiedDataserializable factories. A Factory is a simple dictionary with
        entries of class-id : class-constructor-function pairs.

        Example:

            >>> my_factory = {MyPersonClass.CLASS_ID : MyPersonClass, MyAddressClass.CLASS_ID : MyAddressClass}
            >>> serialization_config.data_serializable_factories[FACTORY_ID] = my_factory

        """
        self.portable_factories = {}
        """
        Dictionary of factory-id and corresponding portable factories. A Factory is a simple dictionary with entries of
        class-id : class-constructor-function pairs.

        Example:

            >>> portable_factory = {PortableClass_0.CLASS_ID : PortableClass_0, PortableClass_1.CLASS_ID : PortableClass_1}
            >>> serialization_config.portable_factories[FACTORY_ID] = portable_factory
        """
        self.class_definitions = set()
        """
        Set of all Portable class definitions.
        """
        self.check_class_def_errors = True
        """Configured Portable Class definitions should be validated for errors or not."""
        self.is_big_endian = True
        """Hazelcast Serialization is big endian or not."""
        self.default_integer_type = INTEGER_TYPE.INT
        """
        Python have variable length int/long type. In order to match this with static fixed length Java server, this option
        defines the length of the int/long.
        One of the values of INTEGER_TYPE can be assigned. Please see INTEGER_TYPE documentation for deatils of the options.
        """
        self._global_serializer = None
        self._custom_serializers = {}

    def add_data_serializable_factory(self, factory_id, factory):
        """
        Helper method for adding IdentifiedDataserializable factory
        example:
            >>> my_factory = {MyPersonClass.CLASS_ID : MyPersonClass, MyAddressClass.CLASS_ID : MyAddressClass}
            >>> serialization_config.add_data_serializable_factory(factory_id, factory)
        :param factory_id: factory-id to register
        :param factory: the factory dict of class-id : class-constructor-function
        """
        self.data_serializable_factories[factory_id] = factory

    def add_portable_factory(self, factory_id, factory):
        """
        Helper method for adding Portable factory
        example:
            >>> portable_factory = {PortableClass_0.CLASS_ID : PortableClass_0, PortableClass_1.CLASS_ID : PortableClass_1}
            >>> serialization_config.portable_factories[FACTORY_ID] = portable_factory
        :param factory_id: factory-id to register
        :param factory: the factory dict of class-id : class-constructor-function
        """
        self.portable_factories[factory_id] = factory

    def set_custom_serializer(self, _type, serializer):
        """
        Assign a serializer for the type
        :param _type: the target type of the serializer
        :param serializer: Custom Serializer constructor function
        """
        validate_type(_type)
        validate_serializer(serializer, StreamSerializer)
        self._custom_serializers[_type] = serializer

    @property
    def custom_serializers(self):
        """
        All custom serializers.
        :return: Dictionary of type, custom serializer
        """
        return self._custom_serializers

    @property
    def global_serializer(self):
        """
        The Global serializer property for serialization service. The assigned value should be a class constructor function.
        It handles every object if no other serializer found.
        Global serializers should extend `hazelcast.serializer.api.StreamSerializer`
        """
        return self._global_serializer

    @global_serializer.setter
    def set_global_serializer(self, global_serializer):
        validate_serializer(global_serializer, StreamSerializer)
        self._global_serializer = global_serializer


class NearCacheConfig(object):
    """
    Map Near cache configuration for a specific map by name.
    """

    def __init__(self, name):
        self._name = name
        self.invalidate_on_change = True
        """Should a value is invalidated and removed in case of any map data updating operations such as replace, remove etc."""
        self._in_memory_format = IN_MEMORY_FORMAT.BINARY
        self._time_to_live_seconds = None
        self._max_idle_seconds = None
        self._eviction_policy = EVICTION_POLICY.NONE
        self._eviction_max_size = DEFAULT_MAX_ENTRY_COUNT
        self._eviction_sampling_count = DEFAULT_SAMPLING_COUNT
        self._eviction_sampling_pool_size = DEFAULT_SAMPLING_POOL_SIZE

    @property
    def name(self):
        """Name of the map that this near cache belong. Cannot be None."""
        return self.name

    @name.setter
    def name(self, name):
        if name is None:
            raise ValueError("Name of the map cannot be None")
        self._name = name

    @property
    def in_memory_format(self):
        """Internal representation of the stored data in near cache."""
        return self._in_memory_format

    @in_memory_format.setter
    def in_memory_format(self, in_memory_format=IN_MEMORY_FORMAT.BINARY):
        if in_memory_format not in IN_MEMORY_FORMAT.reverse:
            raise ValueError("Invalid in-memory-format :{}".format(in_memory_format))
        self._in_memory_format = in_memory_format

    @property
    def time_to_live_seconds(self):
        """The maximum number of seconds for each entry to stay in the near cache."""
        return self._time_to_live_seconds

    @time_to_live_seconds.setter
    def time_to_live_seconds(self, time_to_live_seconds):
        if time_to_live_seconds < 0:
            raise ValueError("'time_to_live_seconds' cannot be less than 0")
        self._time_to_live_seconds = time_to_live_seconds

    @property
    def max_idle_seconds(self):
        """Maximum number of seconds each entry can stay in the near cache as untouched (not-read)."""
        return self._max_idle_seconds

    @max_idle_seconds.setter
    def max_idle_seconds(self, max_idle_seconds):
        if max_idle_seconds < 0:
            raise ValueError("'max_idle_seconds' cannot be less than 0")
        self._max_idle_seconds = max_idle_seconds

    @property
    def eviction_policy(self):
        """The eviction policy for the near cache"""
        return self._eviction_policy

    @eviction_policy.setter
    def eviction_policy(self, eviction_policy):
        if eviction_policy not in EVICTION_POLICY.reverse:
            raise ValueError("Invalid eviction_policy :{}".format(eviction_policy))
        self._eviction_policy = eviction_policy

    @property
    def eviction_max_size(self):
        """The limit for number of entries until the eviction start."""
        return self._eviction_max_size

    @eviction_max_size.setter
    def eviction_max_size(self, eviction_max_size):
        if eviction_max_size < 1:
            raise ValueError("'Eviction-max-size' cannot be less than 1")
        self._eviction_max_size = eviction_max_size

    @property
    def eviction_sampling_count(self):
        """The entry count of the samples for the internal eviction sampling algorithm taking samples in each operation."""
        return self._eviction_sampling_count

    @eviction_sampling_count.setter
    def eviction_sampling_count(self, eviction_sampling_count):
        if eviction_sampling_count < 1:
            raise ValueError("'eviction_sampling_count' cannot be less than 1")
        self._eviction_sampling_count = eviction_sampling_count

    @property
    def eviction_sampling_pool_size(self):
        """The size of the internal eviction sampling algorithm has a pool of best candidates for eviction."""
        return self._eviction_sampling_pool_size

    @eviction_sampling_pool_size.setter
    def eviction_sampling_pool_size(self, eviction_sampling_pool_size):
        if eviction_sampling_pool_size < 1:
            raise ValueError("'eviction_sampling_pool_size' cannot be less than 1")
        self._eviction_sampling_pool_size = eviction_sampling_pool_size
