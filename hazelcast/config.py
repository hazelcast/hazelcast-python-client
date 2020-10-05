"""
Hazelcast Client Configuration module contains configuration classes and various constants required to create a ClientConfig.

"""
import logging
import os
import re

from hazelcast.serialization.api import StreamSerializer
from hazelcast.util import validate_type, validate_serializer, enum, TimeUnit, check_not_none

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

* NONE : No eviction
* LRU : Least Recently Used items will be evicted
* LFU : Least frequently Used items will be evicted
* RANDOM : Items will be evicted randomly

"""

IN_MEMORY_FORMAT = enum(BINARY=0, OBJECT=1)
"""
Near Cache in memory format of the values.

* BINARY : Binary format, hazelcast serialized bytearray format
* OBJECT : The actual objects used
"""

PROTOCOL = enum(SSLv2=0, SSLv3=1, SSL=2, TLSv1=3, TLSv1_1=4, TLSv1_2=5, TLSv1_3=6, TLS=7)
"""
SSL protocol options.

* SSLv2     : SSL 2.O Protocol. RFC 6176 prohibits SSL 2.0. Please use TLSv1+
* SSLv3     : SSL 3.0 Protocol. RFC 7568 prohibits SSL 3.0. Please use TLSv1+
* SSL       : Alias for SSL 3.0
* TLSv1     : TLS 1.0 Protocol described in RFC 2246
* TLSv1_1   : TLS 1.1 Protocol described in RFC 4346
* TLSv1_2   : TLS 1.2 Protocol described in RFC 5246
* TLSv1_3   : TLS 1.3 Protocol described in RFC 8446
* TLS       : Alias for TLS 1.2
* TLSv1+ requires at least Python 2.7.9 or Python 3.4 build with OpenSSL 1.0.1+ 
* TLSv1_3 requires at least Python 2.7.15 or Python 3.7 build with OpenSSL 1.1.1+
"""

QUERY_CONSTANTS = enum(KEY_ATTRIBUTE_NAME="__key", THIS_ATTRIBUTE_NAME="this")
"""
Contains constants for Query.
* KEY_ATTRIBUTE_NAME  : Attribute name of the key.
* THIS_ATTRIBUTE_NAME : Attribute name of the "this"
"""

UNIQUE_KEY_TRANSFORMATION = enum(OBJECT=0, LONG=1, RAW=2)
"""
Defines an assortment of transformations which can be applied to 
BitmapIndexOptions#getUniqueKey() unique key values.
* OBJECT : Extracted unique key value is interpreted as an object value. 
    Non-negative unique ID is assigned to every distinct object value.
* LONG   : Extracted unique key value is interpreted as a whole integer value of byte, short, int or long type. 
    The extracted value is upcasted to long (if necessary) and unique non-negative ID is assigned 
    to every distinct value.
* RAW    : Extracted unique key value is interpreted as a whole integer value of byte, short, int or long type. 
    The extracted value is upcasted to long (if necessary) and the resulting value is used directly as an ID.
"""

INDEX_TYPE = enum(SORTED=0, HASH=1, BITMAP=2)
"""
Type of the index.
* SORTED : Sorted index. Can be used with equality and range predicates.
* HASH   : Hash index. Can be used with equality predicates.
* BITMAP : Bitmap index. Can be used with equality predicates.
"""

_DEFAULT_CLUSTER_NAME = "dev"

_DEFAULT_MAX_ENTRY_COUNT = 10000
_DEFAULT_SAMPLING_COUNT = 8
_DEFAULT_SAMPLING_POOL_SIZE = 16

_MAXIMUM_PREFETCH_COUNT = 100000


class ClientConfig(object):
    """
    The root configuration for hazelcast python client.

        >>> client_config = ClientConfig()
        >>> client = HazelcastClient(client_config)
    """

    def __init__(self):
        self.client_name = None
        """Name of the client"""

        self.cluster_name = _DEFAULT_CLUSTER_NAME
        """Name of the cluster to connect to. By default, set to `dev`."""

        self.network = ClientNetworkConfig()
        """The network configuration for addresses to connect, smart-routing, socket-options..."""

        self.connection_strategy = ConnectionStrategyConfig()
        """Connection strategy config of the client"""

        self.serialization = SerializationConfig()
        """Hazelcast serialization configuration"""

        self.near_caches = {}  # map_name:NearCacheConfig
        """Near Cache configuration which maps "map-name : NearCacheConfig"""

        self._properties = {}
        """Config properties"""

        self.load_balancer = None
        """Custom load balancer used to distribute the operations to multiple Endpoints."""

        self.membership_listeners = []
        """Membership listeners, an array of tuple (member_added, member_removed, fire_for_existing)"""

        self.lifecycle_listeners = []
        """ Lifecycle Listeners, an array of Functions of f(state)"""

        self.flake_id_generators = {}
        """Flake ID generator configuration which maps "config-name" : FlakeIdGeneratorConfig """

        self.logger = LoggerConfig()
        """Logger configuration."""

        self.labels = set()
        """Labels for the client to be sent to the cluster."""

    def add_membership_listener(self, member_added=None, member_removed=None, fire_for_existing=False):
        """
        Helper method for adding membership listeners

        :param member_added: (Function), Function to be called when a member is added, in the form of f(member)
        (optional).
        :param member_removed: (Function), Function to be called when a member is removed, in the form of f(member)
        (optional).
        :param fire_for_existing: if True, already existing members will fire member_added event (optional).
        :return: `self` for cascading configuration
        """
        self.membership_listeners.append((member_added, member_removed, fire_for_existing))
        return self

    def add_lifecycle_listener(self, lifecycle_state_changed=None):
        """
        Helper method for adding lifecycle listeners.

        :param lifecycle_state_changed: (Function), Function to be called when lifecycle state is changed (optional).
        In the form of f(state).
        :return: `self` for cascading configuration
        """
        if lifecycle_state_changed:
            self.lifecycle_listeners.append(lifecycle_state_changed)
        return self

    def add_near_cache_config(self, near_cache_config):
        """
        Helper method to add a new NearCacheConfig.

        :param near_cache_config: (NearCacheConfig), the near_cache config to add.
        :return: `self` for cascading configuration.
        """
        self.near_caches[near_cache_config.name] = near_cache_config
        return self

    def add_flake_id_generator_config(self, flake_id_generator_config):
        """
        Helper method to add a new FlakeIdGeneratorConfig.

        :param flake_id_generator_config: (FlakeIdGeneratorConfig), the configuration to add
        :return: `self` for cascading configuration
        """
        self.flake_id_generators[flake_id_generator_config.name] = flake_id_generator_config
        return self

    def get_property_or_default(self, key, default):
        """
        Client property accessor with fallback to default value.

        :param key: (Object), property key to access.
        :param default: (Object), the default value for fallback.
        :return: (Object), property value if it exist or the default value otherwise.
        """
        try:
            return self._properties[key]
        except KeyError:
            return default

    def get_properties(self):
        """
        Gets the configuration properties.

        :return: (dict), Client configuration properties.
        """
        return self._properties

    def set_property(self, key, value):
        """
        Sets the value of a named property.

        :param key: Property name
        :param value: Value of the property
        :return: `self` for cascading configuration.
        """
        self._properties[key] = value
        return self


class ClientNetworkConfig(object):
    """
    Network related configuration parameters.
    """

    def __init__(self):
        self.addresses = []
        """The candidate address list that client will use to establish initial connection
        
            >>> addresses.append("127.0.0.1:5701")
        """

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
        This can be because of network, or simply because the member died. However it is not clear whether the
        application is performed or not. For idempotent operations this is harmless, but for non idempotent ones
        retrying can cause to undesirable effects. Note that the redo can perform on any member.
        """

        self.smart_routing = True
        """
        If true, client will route the key based operations to owner of the key at the best effort. Note that it uses a
        cached value of partition count and doesn't guarantee that the operation will always be executed on the owner.
        The cached table is updated every 10 seconds.
        """

        self.ssl = SSLConfig()
        """SSL configurations for the client."""

        self.cloud = ClientCloudConfig()
        """Hazelcast Cloud configuration to let the client connect the cluster via Hazelcast.cloud"""


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
        Dictionary of factory-id and corresponding IdentifiedDataserializable factories. A Factory is a simple
        dictionary with entries of class-id : class-constructor-function pairs.

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
        Python has variable length int/long type. In order to match this with static fixed length Java server, this option
        defines the length of the int/long.
        One of the values of :const:`INTEGER_TYPE` can be assigned. Please see :const:`INTEGER_TYPE` documentation for details of the options.
        """

        self._global_serializer = None
        self._custom_serializers = {}

    def add_data_serializable_factory(self, factory_id, factory):
        """
        Helper method for adding IdentifiedDataSerializable factory.
        example:
            >>> my_factory = {MyPersonClass.CLASS_ID : MyPersonClass, MyAddressClass.CLASS_ID : MyAddressClass}
            >>> serialization_config.add_data_serializable_factory(factory_id, my_factory)

        :param factory_id: (int), factory-id to register.
        :param factory: (Dictionary), the factory dictionary of class-id:class-constructor-function pairs.
        """
        self.data_serializable_factories[factory_id] = factory

    def add_portable_factory(self, factory_id, factory):
        """
        Helper method for adding Portable factory.
        example:
            >>> portable_factory = {PortableClass_0.CLASS_ID : PortableClass_0, PortableClass_1.CLASS_ID : PortableClass_1}
            >>> serialization_config.portable_factories[FACTORY_ID] = portable_factory

        :param factory_id: (int), factory-id to register.
        :param factory: (Dictionary), the factory dictionary of class-id:class-constructor-function pairs.
        """
        self.portable_factories[factory_id] = factory

    def set_custom_serializer(self, _type, serializer):
        """
        Assign a serializer for the type.

        :param _type: (Type), the target type of the serializer
        :param serializer: (Serializer), Custom Serializer constructor function
        """
        validate_type(_type)
        validate_serializer(serializer, StreamSerializer)
        self._custom_serializers[_type] = serializer

    @property
    def custom_serializers(self):
        """
        All custom serializers.

        :return: (Dictionary), dictionary of type-custom serializer pairs.
        """
        return self._custom_serializers

    @property
    def global_serializer(self):
        """
        The Global serializer property for serialization service. The assigned value should be a class constructor
        function. It handles every object if no other serializer found.

        Global serializers should extend `hazelcast.serializer.api.StreamSerializer`
        """
        return self._global_serializer

    @global_serializer.setter
    def global_serializer(self, global_serializer):
        validate_serializer(global_serializer, StreamSerializer)
        self._global_serializer = global_serializer


class NearCacheConfig(object):
    """
    Map Near cache configuration for a specific map by name.
    """

    def __init__(self, name="default"):
        self._name = name
        self.invalidate_on_change = True
        """Should a value is invalidated and removed in case of any map data 
        updating operations such as replace, remove etc.
        """

        self._in_memory_format = IN_MEMORY_FORMAT.BINARY
        self._time_to_live_seconds = None
        self._max_idle_seconds = None
        self._eviction_policy = EVICTION_POLICY.LRU
        self._eviction_max_size = _DEFAULT_MAX_ENTRY_COUNT
        self._eviction_sampling_count = _DEFAULT_SAMPLING_COUNT
        self._eviction_sampling_pool_size = _DEFAULT_SAMPLING_POOL_SIZE

    @property
    def name(self):
        """Name of the map that this near cache belong. Cannot be None."""
        return self._name

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
        """The entry count of the samples for the internal eviction sampling algorithm taking samples in each
        operation."""
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


RECONNECT_MODE = enum(OFF=0, ON=1, ASYNC=2)
"""
* OFF   : Prevent reconnect to cluster after a disconnect.
* ON    : Reconnect to cluster by blocking invocations.
* ASYNC : Reconnect to cluster without blocking invocations. Invocations will receive ClientOfflineError
"""


class ConnectionStrategyConfig(object):
    """Connection strategy configuration is used for setting custom strategies and configuring strategy parameters."""

    def __init__(self):
        self.async_start = False
        """Enables non-blocking start mode of HazelcastClient. When set to True, the client 
        creation will not wait to connect to cluster. The client instance will throw exceptions
        until it connects to cluster and becomes ready. If set to False, HazelcastClient will block
        until a cluster connection established and it is ready to use the client instance.
        By default, set to False.
        """

        self.reconnect_mode = RECONNECT_MODE.ON
        """Defines how a client reconnects to cluster after a disconnect."""

        self.connection_retry = ConnectionRetryConfig()
        """Connection retry config to be used by the client."""


_DEFAULT_INITIAL_BACKOFF = 1
_DEFAULT_MAX_BACKOFF = 30
_DEFAULT_CLUSTER_CONNECT_TIMEOUT = 20
_DEFAULT_MULTIPLIER = 1
_DEFAULT_JITTER = 0


class ConnectionRetryConfig(object):
    """Connection retry config controls the period among connection establish retries
    and defines when the client should give up retrying. Supports exponential behaviour
    with jitter for wait periods.
    """

    def __init__(self):
        self.initial_backoff = _DEFAULT_INITIAL_BACKOFF
        """Defines wait period in seconds after the first failure before retrying.
        Must be non-negative. By default, set to 1.
        """

        self.max_backoff = _DEFAULT_MAX_BACKOFF
        """Defines an upper bound for the backoff interval in seconds. Must be non-negative. 
        By default, set to 30 seconds.
        """

        self.cluster_connect_timeout = _DEFAULT_CLUSTER_CONNECT_TIMEOUT
        """Defines timeout value in seconds for the client to give up a connection
        attempt to the cluster. Must be non-negative. By default, set to 20 seconds.
        """

        self.multiplier = _DEFAULT_MULTIPLIER
        """Defines the factor with which to multiply backoff after a failed retry.
        Must be greater than or equal to 1. By default, set to 1.
        """

        self.jitter = _DEFAULT_JITTER
        """Defines how much to randomize backoffs. At each iteration the calculated
        back-off is randomized via following method in pseudo-code
        Random(-jitter * current_backoff, jitter * current_backoff).
        Must be in range [0.0, 1.0]. By default, set to `0` (no randomization)."""


class SSLConfig(object):
    """
    SSL configuration.
    """

    def __init__(self):
        self.enabled = False
        """Enables/disables SSL."""

        self.cafile = None
        """
        Absolute path of concatenated CA certificates used to validate server's certificates in PEM format.
        When SSL is enabled and cafile is not set, a set of default CA certificates from default locations
        will be used.
        """

        self.certfile = None
        """Absolute path of the client certificate in PEM format."""

        self.keyfile = None
        """
        Absolute path of the private key file for the client certificate in the PEM format.
        If this parameter is None, private key will be taken from certfile.
        """

        self.password = None
        """
        Password for decrypting the keyfile if it is encrypted.
        The password may be a function to call to get the password.
        It will be called with no arguments, and it should return a string, bytes, or bytearray.
        If the return value is a string it will be encoded as UTF-8 before using it to decrypt the key.
        Alternatively a string, bytes, or bytearray value may be supplied directly as the password.
        """

        self.protocol = PROTOCOL.TLS
        """Protocol version used in SSL communication. Default value is TLSv1.2"""

        self.ciphers = None
        """
        String in the OpenSSL cipher list format to set the available ciphers for sockets.
        More than one cipher can be set by separating them with a colon.
        """


class FlakeIdGeneratorConfig(object):
    """
    FlakeIdGeneratorConfig contains the configuration for the client regarding
    :class:`~hazelcast.proxy.flake_id_generator.FlakeIdGenerator`
    """

    def __init__(self, name="default"):
        self._name = name
        self._prefetch_count = 100
        self._prefetch_validity_in_millis = 600000

    @property
    def name(self):
        """
        Name of the flake ID generator configuration.

        :getter: Returns the configuration name. This can be actual object name or pattern.
        :setter: Sets the name or name pattern for this config. Must not be modified after this
            instance is added to configuration.
        :type: str
        """
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def prefetch_count(self):
        """
        Prefetch value count.

        :getter: Returns the prefetch count. Prefetch count is in the range 1..100,000.
        :setter: Sets how many IDs are pre-fetched on the background when a new flake ID is requested
            from members. Default is 100.
            Prefetch count should be in the range 1..100,000.
        :type: int
        """
        return self._prefetch_count

    @prefetch_count.setter
    def prefetch_count(self, prefetch_count):
        if not (0 < prefetch_count <= _MAXIMUM_PREFETCH_COUNT):
            raise ValueError("Prefetch count must be 1..{}, not {}".format(_MAXIMUM_PREFETCH_COUNT, prefetch_count))
        self._prefetch_count = prefetch_count

    @property
    def prefetch_validity_in_millis(self):
        """
        Prefetch validity in milliseconds.

        :getter: Returns the prefetch validity in milliseconds.
        :setter: Sets for how long the pre-fetched IDs can be used.
            If this time elapses, a new batch of IDs will be fetched.
            Time unit is milliseconds, default is 600,000 (10 minutes).
            The IDs contain timestamp component, which ensures rough global ordering of IDs.
            If an ID is assigned to an object that was created much later, it will be much out of order.
            If you don't care about ordering, set this value to 0.
        :type: int
        """
        return self._prefetch_validity_in_millis

    @prefetch_validity_in_millis.setter
    def prefetch_validity_in_millis(self, prefetch_validity_in_millis):
        self._prefetch_validity_in_millis = prefetch_validity_in_millis


class ClientCloudConfig(object):
    """
    Hazelcast Cloud configuration to let the client connect the cluster via Hazelcast.cloud
    """

    def __init__(self):
        self.enabled = False
        """Enables/disables cloud config."""

        self.discovery_token = ""
        """Hazelcast Cloud Discovery token of your cluster."""


class LoggerConfig(object):
    """
    Custom configuration for logging or a logging level for the default
    Hazelcast client logger can be set using this class.
    """
    def __init__(self):
        self.config_file = None
        """
        If the configuration file is set, given configuration file
        will be used instead of the default logger configuration
        with the given log level. This should be the absolute 
        path of a JSON file that follows the 
        ``Configuration dictionary schema`` described in the logging 
        module of the standard library.
        """

        self.level = logging.INFO
        """
        Sets the logging level for the default logging
        configuration. To turn off the logging, level
        can be set to a high integer value. If custom
        logging levels are not used, a value greater 
        than 50 is enough to turn off the default
        logger. 
        """


class BitmapIndexOptions(object):
    """
    Configures indexing options specific to bitmap indexes
    """

    def __init__(self, unique_key=QUERY_CONSTANTS.KEY_ATTRIBUTE_NAME,
                 unique_key_transformation=UNIQUE_KEY_TRANSFORMATION.OBJECT):
        self.unique_key = unique_key
        """
        Source of values which uniquely identify each entry being inserted into an index.
        """

        self.unique_key_transformation = unique_key_transformation
        """
        Unique key transformation configured in this index. The transformation is 
        applied to every value extracted from unique key attribute
        """

    def __repr__(self):
        return "BitmapIndexOptions(unique_key=%s, unique_key_transformation=%s)" \
               % (self.unique_key, self.unique_key_transformation)


class IndexConfig(object):
    """
    Configuration of an index. Hazelcast support two types of indexes: sorted index and hash index.
    Sorted indexes could be used with equality and range predicates and have logarithmic search time.
    Hash indexes could be used with equality predicates and have constant search time assuming the hash
    function of the indexed field disperses the elements properly.
    Index could be created on one or more attributes.
    """

    def __init__(self, name=None, type=INDEX_TYPE.SORTED, attributes=None, bitmap_index_options=None):
        self.name = name
        """Name of the index"""

        self.type = type
        """Type of the index"""

        self.attributes = attributes or []
        """Indexed attributes"""

        self.bitmap_index_options = bitmap_index_options or BitmapIndexOptions()
        """Bitmap index options"""

    def add_attribute(self, attribute):
        _IndexUtil.validate_attribute(attribute)
        self.attributes.append(attribute)

    def __repr__(self):
        return "IndexConfig(name=%s, type=%s, attributes=%s, bitmap_index_options=%s)" \
               % (self.name, self.type, self.attributes, self.bitmap_index_options)


class _IndexUtil(object):
    _MAX_ATTRIBUTES = 255
    """Maximum number of attributes allowed in the index."""

    _THIS_PATTERN = re.compile(r"^this\.")
    """Pattern to stripe away "this." prefix."""

    @staticmethod
    def validate_attribute(attribute):
        check_not_none(attribute, "Attribute name cannot be None")

        stripped_attribute = attribute.strip()
        if not stripped_attribute:
            raise ValueError("Attribute name cannot be empty")

        if stripped_attribute.endswith("."):
            raise ValueError("Attribute name cannot end with dot: %s" % attribute)

    @staticmethod
    def validate_and_normalize(map_name, index_config):
        original_attributes = index_config.attributes
        if not original_attributes:
            raise ValueError("Index must have at least one attribute: %s" % index_config)

        if len(original_attributes) > _IndexUtil._MAX_ATTRIBUTES:
            raise ValueError("Index cannot have more than %s attributes %s" % (_IndexUtil._MAX_ATTRIBUTES, index_config))

        if index_config.type == INDEX_TYPE.BITMAP and len(original_attributes) > 1:
            raise ValueError("Composite bitmap indexes are not supported: %s" % index_config)

        normalized_attributes = []
        for original_attribute in original_attributes:
            _IndexUtil.validate_attribute(original_attribute)

            original_attribute = original_attribute.strip()
            normalized_attribute = _IndexUtil.canonicalize_attribute(original_attribute)

            try:
                idx = normalized_attributes.index(normalized_attribute)
            except ValueError:
                pass
            else:
                duplicate_original_attribute = original_attributes[idx]
                if duplicate_original_attribute == original_attribute:
                    raise ValueError("Duplicate attribute name [attribute_name=%s, index_config=%s]"
                                     % (original_attribute, index_config))
                else:
                    raise ValueError("Duplicate attribute names [attribute_name1=%s, attribute_name2=%s, "
                                     "index_config=%s]"
                                     % (duplicate_original_attribute, original_attribute, index_config))

            normalized_attributes.append(normalized_attribute)

        name = index_config.name
        if name and not name.strip():
            name = None

        normalized_config = _IndexUtil.build_normalized_config(map_name, index_config.type, name,
                                                               normalized_attributes)
        if index_config.type == INDEX_TYPE.BITMAP:
            unique_key = index_config.bitmap_index_options.unique_key
            unique_key_transformation = index_config.bitmap_index_options.unique_key_transformation
            _IndexUtil.validate_attribute(unique_key)
            unique_key = _IndexUtil.canonicalize_attribute(unique_key)
            normalized_config.bitmap_index_options.unique_key = unique_key
            normalized_config.bitmap_index_options.unique_key_transformation = unique_key_transformation

        return normalized_config

    @staticmethod
    def canonicalize_attribute(attribute):
        return re.sub(_IndexUtil._THIS_PATTERN, "", attribute)

    @staticmethod
    def build_normalized_config(map_name, index_type, index_name, normalized_attributes):
        new_config = IndexConfig()
        new_config.type = index_type

        name = map_name + "_" + _IndexUtil._index_type_to_name(index_type) if index_name is None else None
        for normalized_attribute in normalized_attributes:
            new_config.add_attribute(normalized_attribute)
            if name:
                name += "_" + normalized_attribute

        if name:
            index_name = name

        new_config.name = index_name
        return new_config

    @staticmethod
    def _index_type_to_name(index_type):
        if index_type == INDEX_TYPE.SORTED:
            return "sorted"
        elif index_type == INDEX_TYPE.HASH:
            return "hash"
        elif index_type == INDEX_TYPE.BITMAP:
            return "bitmap"
        else:
            raise ValueError("Unsupported index type %s" % index_type)


class ClientProperty(object):
    """
    Client property holds the name, default value and time unit of Hazelcast client properties.
    Client properties can be set by

    * Programmatic Configuration
    * Environment variables
    """

    def __init__(self, name, default_value=None, time_unit=None):
        self.name = name
        self.default_value = default_value
        self.time_unit = time_unit


class ClientProperties(object):
    HEARTBEAT_INTERVAL = ClientProperty("hazelcast.client.heartbeat.interval", 5000, TimeUnit.MILLISECOND)
    """
    Time interval between the heartbeats sent by the client to the nodes.
    """

    HEARTBEAT_TIMEOUT = ClientProperty("hazelcast.client.heartbeat.timeout", 60000, TimeUnit.MILLISECOND)
    """
    Client sends heartbeat messages to the members and this is the timeout for this sending operations.
    If there is not any message passing between the client and member within the given time via this property
    in milliseconds, the connection will be closed.
    """

    INVOCATION_TIMEOUT_SECONDS = ClientProperty("hazelcast.client.invocation.timeout.seconds", 120, TimeUnit.SECOND)
    """
    When an invocation gets an exception because
    
    * Member throws an exception.
    * Connection between the client and member is closed.
    * Client's heartbeat requests are timed out.
    
    Time passed since invocation started is compared with this property.
    If the time is already passed, then the exception is delegated to the user. If not, the invocation is retried.
    Note that, if invocation gets no exception and it is a long running one, then it will not get any exception,
    no matter how small this timeout is set.
    """

    INVOCATION_RETRY_PAUSE_MILLIS = ClientProperty("hazelcast.client.invocation.retry.pause.millis", 1000,
                                                   TimeUnit.MILLISECOND)
    """
    Pause time between each retry cycle of an invocation in milliseconds.
    """

    HAZELCAST_CLOUD_DISCOVERY_TOKEN = ClientProperty("hazelcast.client.cloud.discovery.token", "")
    """
    Token to use when discovering cluster via Hazelcast.cloud.
    """

    STATISTICS_ENABLED = ClientProperty("hazelcast.client.statistics.enabled", False)
    """
    Used to enable the client statistics collection.
    """

    STATISTICS_PERIOD_SECONDS = ClientProperty("hazelcast.client.statistics.period.seconds", 3, TimeUnit.SECOND)
    """
    Period in seconds to collect statistics.
    """

    SHUFFLE_MEMBER_LIST = ClientProperty("hazelcast.client.shuffle.member.list", True)
    """
    Client shuffles the given member list to prevent all clients to connect to the same node when
    this property is set to true. When it is set to false, the client tries to connect to the nodes
    in the given order.
    """

    def __init__(self, properties):
        self._properties = properties

    def get(self, property):
        """
        Gets the value of the given property. First checks client config properties, then environment variables
        and lastly fall backs to the default value of the property.

        :param property: (:class:`~hazelcast.config.ClientProperty`), Property to get value from
        :return: Value of the given property
        """
        value = self._properties.get(property.name, None)
        if value is not None:
            return value

        value = os.getenv(property.name, None)
        if value is not None:
            return value

        return property.default_value

    def get_bool(self, property):
        """
        Gets the value of the given property as boolean.

        :param property: (:class:`~hazelcast.config.ClientProperty`), Property to get value from
        :return: (bool), Value of the given property
        """
        value = self.get(property)
        if isinstance(value, bool):
            return value
        return value.lower() == "true"

    def get_seconds(self, property):
        """
        Gets the value of the given property in seconds. If the value of the given property is not a number,
        throws TypeError.

        :param property: (:class:`~hazelcast.config.ClientProperty`), Property to get seconds from
        :return: (float), Value of the given property in seconds
        """
        return TimeUnit.to_seconds(self.get(property), property.time_unit)

    def get_seconds_positive_or_default(self, property):
        """
        Gets the value of the given property in seconds. If the value of the given property is not a number,
        throws TypeError. If the value of the given property in seconds is not positive, tries to
        return the default value in seconds.

        :param property: (:class:`~hazelcast.config.ClientProperty`), Property to get seconds from
        :return: (float), Value of the given property in seconds if it is positive.
            Else, value of the default value of given property in seconds.
        """
        seconds = self.get_seconds(property)
        return seconds if seconds > 0 else TimeUnit.to_seconds(property.default_value, property.time_unit)
