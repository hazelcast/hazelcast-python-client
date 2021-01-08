import re

from hazelcast import six
from hazelcast.errors import InvalidConfigurationError
from hazelcast.serialization.api import StreamSerializer, IdentifiedDataSerializable, Portable
from hazelcast.serialization.portable.classdef import ClassDefinition
from hazelcast.util import (
    check_not_none,
    number_types,
    LoadBalancer,
    none_type,
    try_to_get_enum_value,
)


class IntType(object):
    """Integer type options that can be used by serialization service."""

    VAR = 0
    """
    Integer types will be serialized as 8, 16, 32, 64 bit integers
    or as Java BigInteger according to their value. This option may
    cause problems when the Python client is used in conjunction with
    statically typed language clients such as Java or .NET.
    """

    BYTE = 1
    """
    Integer types will be serialized as a 8 bit integer(as Java byte)
    """

    SHORT = 2
    """
    Integer types will be serialized as a 16 bit integer(as Java short)
    """

    INT = 3
    """
    Integer types will be serialized as a 32 bit integer(as Java int)
    """

    LONG = 4
    """
    Integer types will be serialized as a 64 bit integer(as Java long)
    """

    BIG_INT = 5
    """
    Integer types will be serialized as Java BigInteger. This option can
    handle integer types which are less than -2^63 or greater than or
    equal to 2^63. However, when this option is set, serializing/de-serializing
    integer types is costly.
    """


class EvictionPolicy(object):
    """Near Cache eviction policy options."""

    NONE = 0
    """
    No eviction.
    """

    LRU = 1
    """
    Least Recently Used items will be evicted.
    """

    LFU = 2
    """
    Least frequently Used items will be evicted.
    """

    RANDOM = 3
    """
    Items will be evicted randomly.
    """


class InMemoryFormat(object):
    """Near Cache in memory format of the values."""

    BINARY = 0
    """
    As Hazelcast serialized bytearray data.
    """

    OBJECT = 1
    """
    As the actual object.
    """


class SSLProtocol(object):
    """SSL protocol options.

    TLSv1+ requires at least Python 2.7.9 or Python 3.4 build with OpenSSL 1.0.1+
    TLSv1_3 requires at least Python 2.7.15 or Python 3.7 build with OpenSSL 1.1.1+
    """

    SSLv2 = 0
    """
    SSL 2.0 Protocol. RFC 6176 prohibits SSL 2.0. Please use TLSv1+.
    """

    SSLv3 = 1
    """
    SSL 3.0 Protocol. RFC 7568 prohibits SSL 3.0. Please use TLSv1+.
    """

    TLSv1 = 2
    """
    TLS 1.0 Protocol described in RFC 2246.
    """

    TLSv1_1 = 3
    """
    TLS 1.1 Protocol described in RFC 4346.
    """

    TLSv1_2 = 4
    """
    TLS 1.2 Protocol described in RFC 5246.
    """

    TLSv1_3 = 5
    """
    TLS 1.3 Protocol described in RFC 8446.
    """


class QueryConstants(object):
    """Contains constants for Query."""

    KEY_ATTRIBUTE_NAME = "__key"
    """
    Attribute name of the key.
    """

    THIS_ATTRIBUTE_NAME = "this"
    """
    Attribute name of the value.
    """


class UniqueKeyTransformation(object):
    """Defines an assortment of transformations which can be applied to unique key values."""

    OBJECT = 0
    """
    Extracted unique key value is interpreted as an object value. 
    Non-negative unique ID is assigned to every distinct object value.
    """

    LONG = 1
    """
    Extracted unique key value is interpreted as a whole integer value of byte, short, int or long type. 
    The extracted value is up casted to long (if necessary) and unique non-negative ID is assigned 
    to every distinct value.
    """

    RAW = 2
    """
    Extracted unique key value is interpreted as a whole integer value of byte, short, int or long type. 
    The extracted value is up casted to long (if necessary) and the resulting value is used directly as an ID.
    """


class IndexType(object):
    """Type of the index."""

    SORTED = 0
    """
    Sorted index. Can be used with equality and range predicates.
    """

    HASH = 1
    """
    Hash index. Can be used with equality predicates.
    """

    BITMAP = 2
    """
    Bitmap index. Can be used with equality predicates.
    """


class ReconnectMode(object):
    """Reconnect options."""

    OFF = 0
    """
    Prevent reconnect to cluster after a disconnect.
    """

    ON = 1
    """
    Reconnect to cluster by blocking invocations.
    """

    ASYNC = 2
    """
    Reconnect to cluster without blocking invocations. Invocations will receive ClientOfflineError
    """


class BitmapIndexOptions(object):
    __slots__ = ("_unique_key", "_unique_key_transformation")

    def __init__(self, unique_key=None, unique_key_transformation=None):
        self._unique_key = QueryConstants.KEY_ATTRIBUTE_NAME
        if unique_key is not None:
            self.unique_key = unique_key

        self._unique_key_transformation = UniqueKeyTransformation.OBJECT
        if unique_key_transformation is not None:
            self.unique_key_transformation = unique_key_transformation

    @property
    def unique_key(self):
        return self._unique_key

    @unique_key.setter
    def unique_key(self, value):
        self._unique_key = try_to_get_enum_value(value, QueryConstants)

    @property
    def unique_key_transformation(self):
        return self._unique_key_transformation

    @unique_key_transformation.setter
    def unique_key_transformation(self, value):
        self._unique_key_transformation = try_to_get_enum_value(value, UniqueKeyTransformation)

    @classmethod
    def from_dict(cls, d):
        options = cls()
        for k, v in six.iteritems(d):
            try:
                options.__setattr__(k, v)
            except AttributeError:
                raise InvalidConfigurationError(
                    "Unrecognized config option for the bitmap index options: %s" % k
                )
        return options

    def __repr__(self):
        return "BitmapIndexOptions(unique_key=%s, unique_key_transformation=%s)" % (
            self.unique_key,
            self.unique_key_transformation,
        )


class IndexConfig(object):
    __slots__ = ("_name", "_type", "_attributes", "_bitmap_index_options")

    def __init__(self, name=None, type=None, attributes=None, bitmap_index_options=None):
        self._name = name
        if name is not None:
            self.name = name

        self._type = IndexType.SORTED
        if type is not None:
            self.type = type

        self._attributes = []
        if attributes is not None:
            self.attributes = attributes

        self._bitmap_index_options = BitmapIndexOptions()
        if bitmap_index_options is not None:
            self.bitmap_index_options = bitmap_index_options

    def add_attribute(self, attribute):
        IndexUtil.validate_attribute(attribute)
        self.attributes.append(attribute)

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        if isinstance(value, (six.string_types, none_type)):
            self._name = value
        else:
            raise TypeError("name must be a string or None")

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, value):
        self._type = try_to_get_enum_value(value, IndexType)

    @property
    def attributes(self):
        return self._attributes

    @attributes.setter
    def attributes(self, value):
        if isinstance(value, list):
            for attribute in value:
                IndexUtil.validate_attribute(attribute)
            self._attributes = value
        else:
            raise TypeError("attributes must be a list")

    @property
    def bitmap_index_options(self):
        return self._bitmap_index_options

    @bitmap_index_options.setter
    def bitmap_index_options(self, value):
        if isinstance(value, dict):
            self._bitmap_index_options = BitmapIndexOptions.from_dict(value)
        elif isinstance(value, BitmapIndexOptions):
            # This branch should only be taken by the client protocol
            self._bitmap_index_options = value
        else:
            raise TypeError("bitmap_index_options must be a dict")

    @classmethod
    def from_dict(cls, d):
        config = cls()
        for k, v in six.iteritems(d):
            if v is not None:
                try:
                    config.__setattr__(k, v)
                except AttributeError:
                    raise InvalidConfigurationError(
                        "Unrecognized config option for the index config: %s" % k
                    )
        return config

    def __repr__(self):
        return "IndexConfig(name=%s, type=%s, attributes=%s, bitmap_index_options=%s)" % (
            self.name,
            self.type,
            self.attributes,
            self.bitmap_index_options,
        )


class IndexUtil(object):
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

        if len(original_attributes) > IndexUtil._MAX_ATTRIBUTES:
            raise ValueError(
                "Index cannot have more than %s attributes %s"
                % (IndexUtil._MAX_ATTRIBUTES, index_config)
            )

        if index_config.type == IndexType.BITMAP and len(original_attributes) > 1:
            raise ValueError("Composite bitmap indexes are not supported: %s" % index_config)

        normalized_attributes = []
        for original_attribute in original_attributes:
            IndexUtil.validate_attribute(original_attribute)

            original_attribute = original_attribute.strip()
            normalized_attribute = IndexUtil.canonicalize_attribute(original_attribute)

            try:
                idx = normalized_attributes.index(normalized_attribute)
            except ValueError:
                pass
            else:
                duplicate_original_attribute = original_attributes[idx]
                if duplicate_original_attribute == original_attribute:
                    raise ValueError(
                        "Duplicate attribute name [attribute_name=%s, index_config=%s]"
                        % (original_attribute, index_config)
                    )
                else:
                    raise ValueError(
                        "Duplicate attribute names [attribute_name1=%s, attribute_name2=%s, "
                        "index_config=%s]"
                        % (duplicate_original_attribute, original_attribute, index_config)
                    )

            normalized_attributes.append(normalized_attribute)

        name = index_config.name
        if name and not name.strip():
            name = None

        normalized_config = IndexUtil.build_normalized_config(
            map_name, index_config.type, name, normalized_attributes
        )
        if index_config.type == IndexType.BITMAP:
            unique_key = index_config.bitmap_index_options.unique_key
            unique_key_transformation = index_config.bitmap_index_options.unique_key_transformation
            IndexUtil.validate_attribute(unique_key)
            unique_key = IndexUtil.canonicalize_attribute(unique_key)
            normalized_config.bitmap_index_options.unique_key = unique_key
            normalized_config.bitmap_index_options.unique_key_transformation = (
                unique_key_transformation
            )

        return normalized_config

    @staticmethod
    def canonicalize_attribute(attribute):
        return re.sub(IndexUtil._THIS_PATTERN, "", attribute)

    @staticmethod
    def build_normalized_config(map_name, index_type, index_name, normalized_attributes):
        new_config = IndexConfig()
        new_config.type = index_type

        name = (
            map_name + "_" + IndexUtil._index_type_to_name(index_type)
            if index_name is None
            else None
        )
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
        if index_type == IndexType.SORTED:
            return "sorted"
        elif index_type == IndexType.HASH:
            return "hash"
        elif index_type == IndexType.BITMAP:
            return "bitmap"
        else:
            raise ValueError("Unsupported index type %s" % index_type)


class _Config(object):
    __slots__ = (
        "_cluster_members",
        "_cluster_name",
        "_client_name",
        "_connection_timeout",
        "_socket_options",
        "_redo_operation",
        "_smart_routing",
        "_ssl_enabled",
        "_ssl_cafile",
        "_ssl_certfile",
        "_ssl_keyfile",
        "_ssl_password",
        "_ssl_protocol",
        "_ssl_ciphers",
        "_cloud_discovery_token",
        "_async_start",
        "_reconnect_mode",
        "_retry_initial_backoff",
        "_retry_max_backoff",
        "_retry_jitter",
        "_retry_multiplier",
        "_cluster_connect_timeout",
        "_portable_version",
        "_data_serializable_factories",
        "_portable_factories",
        "_class_definitions",
        "_check_class_definition_errors",
        "_is_big_endian",
        "_default_int_type",
        "_global_serializer",
        "_custom_serializers",
        "_near_caches",
        "_load_balancer",
        "_membership_listeners",
        "_lifecycle_listeners",
        "_flake_id_generators",
        "_labels",
        "_heartbeat_interval",
        "_heartbeat_timeout",
        "_invocation_timeout",
        "_invocation_retry_pause",
        "_statistics_enabled",
        "_statistics_period",
        "_shuffle_member_list",
        "_backup_ack_to_client_enabled",
        "_operation_backup_timeout",
        "_fail_on_indeterminate_operation_state",
    )

    def __init__(self):
        self._cluster_members = []
        self._cluster_name = "dev"
        self._client_name = None
        self._connection_timeout = 5.0
        self._socket_options = []
        self._redo_operation = False
        self._smart_routing = True
        self._ssl_enabled = False
        self._ssl_cafile = None
        self._ssl_certfile = None
        self._ssl_keyfile = None
        self._ssl_password = None
        self._ssl_protocol = SSLProtocol.TLSv1_2
        self._ssl_ciphers = None
        self._cloud_discovery_token = None
        self._async_start = False
        self._reconnect_mode = ReconnectMode.ON
        self._retry_initial_backoff = 1.0
        self._retry_max_backoff = 30.0
        self._retry_jitter = 0.0
        self._retry_multiplier = 1.0
        self._cluster_connect_timeout = 120.0
        self._portable_version = 0
        self._data_serializable_factories = {}
        self._portable_factories = {}
        self._class_definitions = []
        self._check_class_definition_errors = True
        self._is_big_endian = True
        self._default_int_type = IntType.INT
        self._global_serializer = None
        self._custom_serializers = {}
        self._near_caches = {}
        self._load_balancer = None
        self._membership_listeners = []
        self._lifecycle_listeners = []
        self._flake_id_generators = {}
        self._labels = []
        self._heartbeat_interval = 5.0
        self._heartbeat_timeout = 60.0
        self._invocation_timeout = 120.0
        self._invocation_retry_pause = 1.0
        self._statistics_enabled = False
        self._statistics_period = 3.0
        self._shuffle_member_list = True
        self._backup_ack_to_client_enabled = True
        self._operation_backup_timeout = 5.0
        self._fail_on_indeterminate_operation_state = False

    @property
    def cluster_members(self):
        return self._cluster_members

    @cluster_members.setter
    def cluster_members(self, value):
        if isinstance(value, list):
            for address in value:
                if not isinstance(address, six.string_types):
                    raise TypeError("cluster_members must be list of strings")

            self._cluster_members = value
        else:
            raise TypeError("cluster_members must be a list")

    @property
    def cluster_name(self):
        return self._cluster_name

    @cluster_name.setter
    def cluster_name(self, value):
        if isinstance(value, six.string_types):
            self._cluster_name = value
        else:
            raise TypeError("cluster_name must be a string")

    @property
    def client_name(self):
        return self._client_name

    @client_name.setter
    def client_name(self, value):
        if isinstance(value, six.string_types):
            self._client_name = value
        else:
            raise TypeError("client_name must be a string")

    @property
    def connection_timeout(self):
        return self._connection_timeout

    @connection_timeout.setter
    def connection_timeout(self, value):
        if isinstance(value, number_types):
            if value < 0:
                raise ValueError("connection_timeout must be non-negative")
            self._connection_timeout = value
        else:
            raise TypeError("connection_timeout must be a number")

    @property
    def socket_options(self):
        return self._socket_options

    @socket_options.setter
    def socket_options(self, value):
        if isinstance(value, list):
            try:
                for _, _, _ in value:
                    # Must be a tuple of length 3
                    pass

                self._socket_options = value
            except ValueError:
                raise TypeError("socket_options must contain tuples of length 3 as items")
        else:
            raise TypeError("socket_options must be a list")

    @property
    def redo_operation(self):
        return self._redo_operation

    @redo_operation.setter
    def redo_operation(self, value):
        if isinstance(value, bool):
            self._redo_operation = value
        else:
            raise TypeError("redo_operation must be a boolean")

    @property
    def smart_routing(self):
        return self._smart_routing

    @smart_routing.setter
    def smart_routing(self, value):
        if isinstance(value, bool):
            self._smart_routing = value
        else:
            raise TypeError("smart_routing must be a boolean")

    @property
    def ssl_enabled(self):
        return self._ssl_enabled

    @ssl_enabled.setter
    def ssl_enabled(self, value):
        if isinstance(value, bool):
            self._ssl_enabled = value
        else:
            raise TypeError("ssl_enabled must be a boolean")

    @property
    def ssl_cafile(self):
        return self._ssl_cafile

    @ssl_cafile.setter
    def ssl_cafile(self, value):
        if isinstance(value, six.string_types):
            self._ssl_cafile = value
        else:
            raise TypeError("ssl_cafile must be a string")

    @property
    def ssl_certfile(self):
        return self._ssl_certfile

    @ssl_certfile.setter
    def ssl_certfile(self, value):
        if isinstance(value, six.string_types):
            self._ssl_certfile = value
        else:
            raise TypeError("ssl_certfile must be a string")

    @property
    def ssl_keyfile(self):
        return self._ssl_keyfile

    @ssl_keyfile.setter
    def ssl_keyfile(self, value):
        if isinstance(value, six.string_types):
            self._ssl_keyfile = value
        else:
            raise TypeError("ssl_keyfile must be a string")

    @property
    def ssl_password(self):
        return self._ssl_password

    @ssl_password.setter
    def ssl_password(self, value):
        if isinstance(value, (six.string_types, six.binary_type, bytearray)) or callable(value):
            self._ssl_password = value
        else:
            raise TypeError("ssl_password must be string, bytes, bytearray or callable")

    @property
    def ssl_protocol(self):
        return self._ssl_protocol

    @ssl_protocol.setter
    def ssl_protocol(self, value):
        self._ssl_protocol = try_to_get_enum_value(value, SSLProtocol)

    @property
    def ssl_ciphers(self):
        return self._ssl_ciphers

    @ssl_ciphers.setter
    def ssl_ciphers(self, value):
        if isinstance(value, six.string_types):
            self._ssl_ciphers = value
        else:
            raise TypeError("ssl_ciphers must be a string")

    @property
    def cloud_discovery_token(self):
        return self._cloud_discovery_token

    @cloud_discovery_token.setter
    def cloud_discovery_token(self, value):
        if isinstance(value, six.string_types):
            self._cloud_discovery_token = value
        else:
            raise TypeError("cloud_discovery_token must be a string")

    @property
    def async_start(self):
        return self._async_start

    @async_start.setter
    def async_start(self, value):
        if isinstance(value, bool):
            self._async_start = value
        else:
            raise TypeError("async_start must be a boolean")

    @property
    def reconnect_mode(self):
        return self._reconnect_mode

    @reconnect_mode.setter
    def reconnect_mode(self, value):
        self._reconnect_mode = try_to_get_enum_value(value, ReconnectMode)

    @property
    def retry_initial_backoff(self):
        return self._retry_initial_backoff

    @retry_initial_backoff.setter
    def retry_initial_backoff(self, value):
        if isinstance(value, number_types):
            if value < 0:
                raise ValueError("retry_initial_backoff must be non-negative")
            self._retry_initial_backoff = value
        else:
            raise TypeError("retry_initial_backoff must be a number")

    @property
    def retry_max_backoff(self):
        return self._retry_max_backoff

    @retry_max_backoff.setter
    def retry_max_backoff(self, value):
        if isinstance(value, number_types):
            if value < 0:
                raise ValueError("retry_max_backoff must be non-negative")
            self._retry_max_backoff = value
        else:
            raise TypeError("retry_max_backoff must be a number")

    @property
    def retry_jitter(self):
        return self._retry_jitter

    @retry_jitter.setter
    def retry_jitter(self, value):
        if isinstance(value, number_types):
            if value < 0 or value > 1:
                raise ValueError("retry_jitter must be in range [0.0, 1.0]")
            self._retry_jitter = value
        else:
            raise TypeError("retry_jitter must be a number")

    @property
    def retry_multiplier(self):
        return self._retry_multiplier

    @retry_multiplier.setter
    def retry_multiplier(self, value):
        if isinstance(value, number_types):
            if value < 1:
                raise ValueError("retry_multiplier must be greater than or equal to 1.0")
            self._retry_multiplier = value
        else:
            raise TypeError("retry_multiplier must be a number")

    @property
    def cluster_connect_timeout(self):
        return self._cluster_connect_timeout

    @cluster_connect_timeout.setter
    def cluster_connect_timeout(self, value):
        if isinstance(value, number_types):
            if value < 0:
                raise ValueError("cluster_connect_timeout must be non-negative")
            self._cluster_connect_timeout = value
        else:
            raise TypeError("cluster_connect_timeout must be a number")

    @property
    def portable_version(self):
        return self._portable_version

    @portable_version.setter
    def portable_version(self, value):
        if isinstance(value, number_types):
            if value < 0:
                raise ValueError("portable_version must be non-negative")
            self._portable_version = value
        else:
            raise TypeError("portable_version must be a number")

    @property
    def data_serializable_factories(self):
        return self._data_serializable_factories

    @data_serializable_factories.setter
    def data_serializable_factories(self, value):
        if isinstance(value, dict):
            for factory_id, factory in six.iteritems(value):
                if not isinstance(factory_id, six.integer_types):
                    raise TypeError("Keys of data_serializable_factories must be integers")

                if not isinstance(factory, dict):
                    raise TypeError("Values of data_serializable_factories must be dict")

                for class_id, clazz in six.iteritems(factory):
                    if not isinstance(class_id, six.integer_types):
                        raise TypeError(
                            "Keys of factories of data_serializable_factories must be integers"
                        )

                    if not (
                        isinstance(clazz, type) and issubclass(clazz, IdentifiedDataSerializable)
                    ):
                        raise TypeError(
                            "Values of factories of data_serializable_factories must be "
                            "subclasses of IdentifiedDataSerializable"
                        )

            self._data_serializable_factories = value
        else:
            raise TypeError("data_serializable_factories must be a dict")

    @property
    def portable_factories(self):
        return self._portable_factories

    @portable_factories.setter
    def portable_factories(self, value):
        if isinstance(value, dict):
            for factory_id, factory in six.iteritems(value):
                if not isinstance(factory_id, six.integer_types):
                    raise TypeError("Keys of portable_factories must be integers")

                if not isinstance(factory, dict):
                    raise TypeError("Values of portable_factories must be dict")

                for class_id, clazz in six.iteritems(factory):
                    if not isinstance(class_id, six.integer_types):
                        raise TypeError("Keys of factories of portable_factories must be integers")

                    if not (isinstance(clazz, type) and issubclass(clazz, Portable)):
                        raise TypeError(
                            "Values of factories of portable_factories must be "
                            "subclasses of Portable"
                        )

            self._portable_factories = value
        else:
            raise TypeError("portable_factories must be a dict")

    @property
    def class_definitions(self):
        return self._class_definitions

    @class_definitions.setter
    def class_definitions(self, value):
        if isinstance(value, list):
            for cd in value:
                if not isinstance(cd, ClassDefinition):
                    raise TypeError(
                        "class_definitions must contain objects of type ClassDefinition"
                    )

            self._class_definitions = value
        else:
            raise TypeError("class_definitions must be a list")

    @property
    def check_class_definition_errors(self):
        return self._check_class_definition_errors

    @check_class_definition_errors.setter
    def check_class_definition_errors(self, value):
        if isinstance(value, bool):
            self._check_class_definition_errors = value
        else:
            raise TypeError("check_class_definition_errors must be a boolean")

    @property
    def is_big_endian(self):
        return self._is_big_endian

    @is_big_endian.setter
    def is_big_endian(self, value):
        if isinstance(value, bool):
            self._is_big_endian = value
        else:
            raise TypeError("is_big_endian must be a boolean")

    @property
    def default_int_type(self):
        return self._default_int_type

    @default_int_type.setter
    def default_int_type(self, value):
        self._default_int_type = try_to_get_enum_value(value, IntType)

    @property
    def global_serializer(self):
        return self._global_serializer

    @global_serializer.setter
    def global_serializer(self, value):
        if isinstance(value, type) and issubclass(value, StreamSerializer):
            self._global_serializer = value
        else:
            raise TypeError("global_serializer must be a StreamSerializer")

    @property
    def custom_serializers(self):
        return self._custom_serializers

    @custom_serializers.setter
    def custom_serializers(self, value):
        if isinstance(value, dict):
            for _type, serializer in six.iteritems(value):
                if not isinstance(_type, type):
                    raise TypeError("Keys of custom_serializers must be types")

                if not (isinstance(serializer, type) and issubclass(serializer, StreamSerializer)):
                    raise TypeError(
                        "Values of custom_serializers must be subclasses of StreamSerializer"
                    )

            self._custom_serializers = value
        else:
            raise TypeError("custom_serializers must be a dict")

    @property
    def near_caches(self):
        return self._near_caches

    @near_caches.setter
    def near_caches(self, value):
        if isinstance(value, dict):
            configs = dict()
            for name, config in six.iteritems(value):
                if not isinstance(name, six.string_types):
                    raise TypeError("Keys of near_caches must be strings")

                if not isinstance(config, dict):
                    raise TypeError("Values of near_caches must be dict")

                configs[name] = _NearCacheConfig.from_dict(config)

            self._near_caches = configs
        else:
            raise TypeError("near_caches must be a dict")

    @property
    def load_balancer(self):
        return self._load_balancer

    @load_balancer.setter
    def load_balancer(self, value):
        if isinstance(value, LoadBalancer):
            self._load_balancer = value
        else:
            raise TypeError("load_balancer must be a LoadBalancer")

    @property
    def membership_listeners(self):
        return self._membership_listeners

    @membership_listeners.setter
    def membership_listeners(self, value):
        if isinstance(value, list):
            try:
                for item in value:
                    try:
                        added, removed = item
                    except TypeError:
                        raise TypeError(
                            "membership_listeners must contain tuples of length 2 as items"
                        )

                    if not (callable(added) or callable(removed)):
                        raise TypeError(
                            "At least one of the listeners in the tuple most be callable"
                        )

                self._membership_listeners = value
            except ValueError:
                raise TypeError("membership_listeners must contain tuples of length 2 as items")
        else:
            raise TypeError("membership_listeners must be a list")

    @property
    def lifecycle_listeners(self):
        return self._lifecycle_listeners

    @lifecycle_listeners.setter
    def lifecycle_listeners(self, value):
        if isinstance(value, list):
            for listener in value:
                if not callable(listener):
                    raise TypeError("lifecycle_listeners must contain callable items")

            self._lifecycle_listeners = value
        else:
            raise TypeError("lifecycle_listeners must be a list")

    @property
    def flake_id_generators(self):
        return self._flake_id_generators

    @flake_id_generators.setter
    def flake_id_generators(self, value):
        if isinstance(value, dict):
            configs = dict()
            for name, config in six.iteritems(value):
                if not isinstance(name, six.string_types):
                    raise TypeError("Keys of flake_id_generators must be strings")

                if not isinstance(config, dict):
                    raise TypeError("Values of flake_id_generators must be dict")

                configs[name] = _FlakeIdGeneratorConfig.from_dict(config)

            self._flake_id_generators = configs
        else:
            raise TypeError("flake_id_generators must be a dict")

    @property
    def labels(self):
        return self._labels

    @labels.setter
    def labels(self, value):
        if isinstance(value, list):
            for label in value:
                if not isinstance(label, six.string_types):
                    raise TypeError("labels must be list of strings")

            self._labels = value
        else:
            raise TypeError("labels must be a list")

    @property
    def heartbeat_interval(self):
        return self._heartbeat_interval

    @heartbeat_interval.setter
    def heartbeat_interval(self, value):
        if isinstance(value, number_types):
            if value <= 0:
                raise ValueError("heartbeat_interval must be positive")
            self._heartbeat_interval = value
        else:
            raise TypeError("heartbeat_interval must be a number")

    @property
    def heartbeat_timeout(self):
        return self._heartbeat_timeout

    @heartbeat_timeout.setter
    def heartbeat_timeout(self, value):
        if isinstance(value, number_types):
            if value <= 0:
                raise ValueError("heartbeat_timeout must be positive")
            self._heartbeat_timeout = value
        else:
            raise TypeError("heartbeat_timeout must be a number")

    @property
    def invocation_timeout(self):
        return self._invocation_timeout

    @invocation_timeout.setter
    def invocation_timeout(self, value):
        if isinstance(value, number_types):
            if value <= 0:
                raise ValueError("invocation_timeout must be positive")
            self._invocation_timeout = value
        else:
            raise TypeError("invocation_timeout must be a number")

    @property
    def invocation_retry_pause(self):
        return self._invocation_retry_pause

    @invocation_retry_pause.setter
    def invocation_retry_pause(self, value):
        if isinstance(value, number_types):
            if value <= 0:
                raise ValueError("invocation_retry_pause must be positive")
            self._invocation_retry_pause = value
        else:
            raise TypeError("invocation_retry_pause must be a number")

    @property
    def statistics_enabled(self):
        return self._statistics_enabled

    @statistics_enabled.setter
    def statistics_enabled(self, value):
        if isinstance(value, bool):
            self._statistics_enabled = value
        else:
            raise TypeError("statistics_enabled must be a boolean")

    @property
    def statistics_period(self):
        return self._statistics_period

    @statistics_period.setter
    def statistics_period(self, value):
        if isinstance(value, number_types):
            if value <= 0:
                raise ValueError("statistics_period must be positive")
            self._statistics_period = value
        else:
            raise TypeError("statistics_period must be a number")

    @property
    def shuffle_member_list(self):
        return self._shuffle_member_list

    @shuffle_member_list.setter
    def shuffle_member_list(self, value):
        if isinstance(value, bool):
            self._shuffle_member_list = value
        else:
            raise TypeError("shuffle_member_list must be a boolean")

    @property
    def backup_ack_to_client_enabled(self):
        return self._backup_ack_to_client_enabled

    @backup_ack_to_client_enabled.setter
    def backup_ack_to_client_enabled(self, value):
        if isinstance(value, bool):
            self._backup_ack_to_client_enabled = value
        else:
            raise TypeError("backup_ack_to_client_enabled must be a boolean")

    @property
    def operation_backup_timeout(self):
        return self._operation_backup_timeout

    @operation_backup_timeout.setter
    def operation_backup_timeout(self, value):
        if isinstance(value, number_types):
            if value > 0:
                self._operation_backup_timeout = value
            else:
                raise ValueError("operation_backup_timeout must be positive")
        else:
            raise TypeError("operation_backup_timeout must be a number")

    @property
    def fail_on_indeterminate_operation_state(self):
        return self._fail_on_indeterminate_operation_state

    @fail_on_indeterminate_operation_state.setter
    def fail_on_indeterminate_operation_state(self, value):
        if isinstance(value, bool):
            self._fail_on_indeterminate_operation_state = value
        else:
            raise TypeError("fail_on_indeterminate_operation_state must be a boolean")

    @classmethod
    def from_dict(cls, d):
        config = cls()
        for k, v in six.iteritems(d):
            if v is not None:
                try:
                    config.__setattr__(k, v)
                except AttributeError:
                    raise InvalidConfigurationError("Unrecognized config option: %s" % k)
        return config


class _NearCacheConfig(object):
    __slots__ = (
        "_invalidate_on_change",
        "_in_memory_format",
        "_time_to_live",
        "_max_idle",
        "_eviction_policy",
        "_eviction_max_size",
        "_eviction_sampling_count",
        "_eviction_sampling_pool_size",
    )

    def __init__(self):
        self._invalidate_on_change = True
        self._in_memory_format = InMemoryFormat.BINARY
        self._time_to_live = None
        self._max_idle = None
        self._eviction_policy = EvictionPolicy.LRU
        self._eviction_max_size = 10000
        self._eviction_sampling_count = 8
        self._eviction_sampling_pool_size = 16

    @property
    def invalidate_on_change(self):
        return self._invalidate_on_change

    @invalidate_on_change.setter
    def invalidate_on_change(self, value):
        if isinstance(value, bool):
            self._invalidate_on_change = value
        else:
            raise TypeError("invalidate_on_change must be a boolean")

    @property
    def in_memory_format(self):
        return self._in_memory_format

    @in_memory_format.setter
    def in_memory_format(self, value):
        self._in_memory_format = try_to_get_enum_value(value, InMemoryFormat)

    @property
    def time_to_live(self):
        return self._time_to_live

    @time_to_live.setter
    def time_to_live(self, value):
        if isinstance(value, number_types):
            if value < 0:
                raise ValueError("time_to_live must be non-negative")
            self._time_to_live = value
        else:
            raise TypeError("time_to_live must be a number")

    @property
    def max_idle(self):
        return self._max_idle

    @max_idle.setter
    def max_idle(self, value):
        if isinstance(value, number_types):
            if value < 0:
                raise ValueError("max_idle must be non-negative")
            self._max_idle = value
        else:
            raise TypeError("max_idle must be a number")

    @property
    def eviction_policy(self):
        return self._eviction_policy

    @eviction_policy.setter
    def eviction_policy(self, value):
        self._eviction_policy = try_to_get_enum_value(value, EvictionPolicy)

    @property
    def eviction_max_size(self):
        return self._eviction_max_size

    @eviction_max_size.setter
    def eviction_max_size(self, value):
        if isinstance(value, number_types):
            if value < 1:
                raise ValueError("eviction_max_size must be greater than 1")
            self._eviction_max_size = value
        else:
            raise TypeError("eviction_max_size must be a number")

    @property
    def eviction_sampling_count(self):
        return self._eviction_sampling_count

    @eviction_sampling_count.setter
    def eviction_sampling_count(self, value):
        if isinstance(value, number_types):
            if value < 1:
                raise ValueError("eviction_sampling_count must be greater than 1")
            self._eviction_sampling_count = value
        else:
            raise TypeError("eviction_sampling_count must be a number")

    @property
    def eviction_sampling_pool_size(self):
        return self._eviction_sampling_pool_size

    @eviction_sampling_pool_size.setter
    def eviction_sampling_pool_size(self, value):
        if isinstance(value, number_types):
            if value < 1:
                raise ValueError("eviction_sampling_pool_size must be greater than 1")
            self._eviction_sampling_pool_size = value
        else:
            raise TypeError("eviction_sampling_pool_size must be a number")

    @classmethod
    def from_dict(cls, d):
        config = cls()
        for k, v in six.iteritems(d):
            try:
                config.__setattr__(k, v)
            except AttributeError:
                raise InvalidConfigurationError(
                    "Unrecognized config option for the near cache: %s" % k
                )
        return config


class _FlakeIdGeneratorConfig(object):
    __slots__ = ("_prefetch_count", "_prefetch_validity")

    def __init__(self):
        self._prefetch_count = 100
        self._prefetch_validity = 600

    @property
    def prefetch_count(self):
        return self._prefetch_count

    @prefetch_count.setter
    def prefetch_count(self, value):
        if isinstance(value, number_types):
            if not (0 < value <= 100000):
                raise ValueError("prefetch_count must be in range 1 to 100000")
            self._prefetch_count = value
        else:
            raise TypeError("prefetch_count must be a number")

    @property
    def prefetch_validity(self):
        return self._prefetch_validity

    @prefetch_validity.setter
    def prefetch_validity(self, value):
        if isinstance(value, number_types):
            if value < 0:
                raise ValueError("prefetch_validity must be non-negative")
            self._prefetch_validity = value
        else:
            raise TypeError("prefetch_validity must be a number")

    @classmethod
    def from_dict(cls, d):
        config = cls()
        for k, v in six.iteritems(d):
            try:
                config.__setattr__(k, v)
            except AttributeError:
                raise InvalidConfigurationError(
                    "Unrecognized config option for the flake id generator: %s" % k
                )
        return config
