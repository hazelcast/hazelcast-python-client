import re
import types
import typing

from hazelcast.core import MemberInfo
from hazelcast.errors import InvalidConfigurationError
from hazelcast.serialization.api import (
    StreamSerializer,
    IdentifiedDataSerializable,
    Portable,
    CompactSerializer,
)
from hazelcast.serialization.portable.classdef import ClassDefinition
from hazelcast.security import TokenProvider
from hazelcast.util import (
    check_not_none,
    number_types,
    LoadBalancer,
    try_to_get_enum_value,
)


class IntType:
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


class EvictionPolicy:
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


class InMemoryFormat:
    """Near Cache in memory format of the values."""

    BINARY = 0
    """
    As Hazelcast serialized bytearray data.
    """

    OBJECT = 1
    """
    As the actual object.
    """


class SSLProtocol:
    """SSL protocol options.

    TLSv1_3 requires at least Python 3.7 build with OpenSSL 1.1.1+
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


class QueryConstants:
    """Contains constants for Query."""

    KEY_ATTRIBUTE_NAME = "__key"
    """
    Attribute name of the key.
    """

    THIS_ATTRIBUTE_NAME = "this"
    """
    Attribute name of the value.
    """


class UniqueKeyTransformation:
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


class IndexType:
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


class ReconnectMode:
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


class TopicOverloadPolicy:
    """A policy to deal with an overloaded topic; a topic where there is no
    place to store new messages.

    The reliable topic uses a :class:`hazelcast.proxy.ringbuffer.Ringbuffer` to
    store the messages. A ringbuffer doesn't track where readers are, so
    it has no concept of a slow consumers. This provides many advantages like
    high performance reads, but it also gives the ability to the reader to
    re-read the same message multiple times in case of an error.

    A ringbuffer has a limited, fixed capacity. A fast producer may overwrite
    old messages that are still being read by a slow consumer. To prevent
    this, we may configure a time-to-live on the ringbuffer.

    Once the time-to-live is configured, the :class:`TopicOverloadPolicy`
    controls how the publisher is going to deal with the situation that a
    ringbuffer is full and the oldest item in the ringbuffer is not old
    enough to get overwritten.

    Keep in mind that this retention period (time-to-live) can keep messages
    from being overwritten, even though all readers might have already completed
    reading.
    """

    DISCARD_OLDEST = 0
    """Using this policy, a message that has not expired can be overwritten.
    
    No matter the retention period set, the overwrite will just overwrite
    the item.
    
    This can be a problem for slow consumers because they were promised a
    certain time window to process messages. But it will benefit producers
    and fast consumers since they are able to continue. This policy sacrifices
    the slow producer in favor of fast producers/consumers.
    """

    DISCARD_NEWEST = 1
    """The message that was to be published is discarded."""

    BLOCK = 2
    """The caller will wait till there space in the ringbuffer."""

    ERROR = 3
    """The publish call immediately fails."""


_DEFAULT_CLUSTER_NAME = "dev"
_DEFAULT_CONNECTION_TIMEOUT = 5.0
_DEFAULT_RETRY_INITIAL_BACKOFF = 1.0
_DEFAULT_RETRY_MAX_BACKOFF = 30.0
_DEFAULT_RETRY_JITTER = 0.0
_DEFAULT_RETRY_MULTIPLIER = 1.05
_DEFAULT_CLUSTER_CONNECT_TIMEOUT = -1
_DEFAULT_PORTABLE_VERSION = 0
_DEFAULT_HEARTBEAT_INTERVAL = 5.0
_DEFAULT_HEARTBEAT_TIMEOUT = 60.0
_DEFAULT_INVOCATION_TIMEOUT = 120.0
_DEFAULT_INVOCATION_RETRY_PAUSE = 1.0
_DEFAULT_STATISTICS_PERIOD = 3.0
_DEFAULT_OPERATION_BACKUP_TIMEOUT = 5.0

_MembershipListenerType = typing.Optional[typing.Callable[[MemberInfo], None]]
_Numeric = typing.Union[int, float]


class Config:
    """Hazelcast client configuration."""

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
        "_ssl_check_hostname",
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
        "_compact_serializers",
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
        "_reliable_topics",
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
        "_creds_username",
        "_creds_password",
        "_token_provider",
        "_use_public_ip",
    )

    def __init__(self):
        self._cluster_members: typing.List[str] = []
        self._cluster_name: str = _DEFAULT_CLUSTER_NAME
        self._client_name: typing.Optional[str] = None
        self._connection_timeout: _Numeric = _DEFAULT_CONNECTION_TIMEOUT
        self._socket_options: typing.List[typing.Tuple[int, int, typing.Union[int, bytes]]] = []
        self._redo_operation: bool = False
        self._smart_routing: bool = True
        self._ssl_enabled: bool = False
        self._ssl_cafile: typing.Optional[str] = None
        self._ssl_certfile: typing.Optional[str] = None
        self._ssl_keyfile: typing.Optional[str] = None
        self._ssl_password: typing.Optional[
            typing.Union[typing.Callable[[], typing.Union[str, bytes]], str, bytes]
        ] = None
        self._ssl_protocol: int = SSLProtocol.TLSv1_2
        self._ssl_ciphers: typing.Optional[str] = None
        self._ssl_check_hostname: bool = False
        self._cloud_discovery_token: typing.Optional[str] = None
        self._async_start: bool = False
        self._reconnect_mode: int = ReconnectMode.ON
        self._retry_initial_backoff: _Numeric = _DEFAULT_RETRY_INITIAL_BACKOFF
        self._retry_max_backoff: _Numeric = _DEFAULT_RETRY_MAX_BACKOFF
        self._retry_jitter: _Numeric = _DEFAULT_RETRY_JITTER
        self._retry_multiplier: _Numeric = _DEFAULT_RETRY_MULTIPLIER
        self._cluster_connect_timeout: _Numeric = _DEFAULT_CLUSTER_CONNECT_TIMEOUT
        self._portable_version: int = _DEFAULT_PORTABLE_VERSION
        self._data_serializable_factories: typing.Dict[
            int, typing.Dict[int, typing.Type[IdentifiedDataSerializable]]
        ] = {}
        self._portable_factories: typing.Dict[int, typing.Dict[int, typing.Type[Portable]]] = {}
        self._compact_serializers: typing.List[CompactSerializer] = []
        self._class_definitions: typing.List[ClassDefinition] = []
        self._check_class_definition_errors: bool = True
        self._is_big_endian: bool = True
        self._default_int_type: int = IntType.INT
        self._global_serializer: typing.Optional[typing.Type[StreamSerializer]] = None
        self._custom_serializers: typing.Dict[
            typing.Type[typing.Any], typing.Type[StreamSerializer]
        ] = {}
        self._near_caches: typing.Dict[str, "NearCacheConfig"] = {}
        self._load_balancer: typing.Optional[LoadBalancer] = None
        self._membership_listeners: typing.List[
            typing.Tuple[_MembershipListenerType, _MembershipListenerType]
        ] = []
        self._lifecycle_listeners: typing.List[typing.Callable[[str], None]] = []
        self._flake_id_generators: typing.Dict[str, "FlakeIdGeneratorConfig"] = {}
        self._reliable_topics: typing.Dict[str, "ReliableTopicConfig"] = {}
        self._labels: typing.List[str] = []
        self._heartbeat_interval: _Numeric = _DEFAULT_HEARTBEAT_INTERVAL
        self._heartbeat_timeout: _Numeric = _DEFAULT_HEARTBEAT_TIMEOUT
        self._invocation_timeout: _Numeric = _DEFAULT_INVOCATION_TIMEOUT
        self._invocation_retry_pause: _Numeric = _DEFAULT_INVOCATION_RETRY_PAUSE
        self._statistics_enabled: bool = False
        self._statistics_period: _Numeric = _DEFAULT_STATISTICS_PERIOD
        self._shuffle_member_list: bool = True
        self._backup_ack_to_client_enabled: bool = True
        self._operation_backup_timeout: _Numeric = _DEFAULT_OPERATION_BACKUP_TIMEOUT
        self._fail_on_indeterminate_operation_state: bool = False
        self._creds_username: typing.Optional[str] = None
        self._creds_password: typing.Optional[str] = None
        self._token_provider: typing.Optional[TokenProvider] = None
        self._use_public_ip: bool = False

    @property
    def cluster_members(self) -> typing.List[str]:
        """Candidate address list that the client will use to establish
        initial connection.

        By default, set to ``["127.0.0.1"]``.
        """
        return self._cluster_members

    @cluster_members.setter
    def cluster_members(self, value: typing.List[str]) -> None:
        if not isinstance(value, list):
            raise TypeError("cluster_members must be a list")

        for address in value:
            if not isinstance(address, str):
                raise TypeError("cluster_members must be list of strings")

        self._cluster_members = value

    @property
    def cluster_name(self) -> str:
        """Name of the cluster to connect to.

        The name is sent as part of the client authentication message and may
        be verified on the member. By default, set to ``dev``.
        """
        return self._cluster_name

    @cluster_name.setter
    def cluster_name(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("cluster_name must be a string")

        self._cluster_name = value

    @property
    def client_name(self) -> typing.Optional[str]:
        """Name of the client instance.

        By default, set to ``hz.client_${CLIENT_ID}``, where ``CLIENT_ID``
        starts from ``0`` and it is incremented by ``1`` for each new client.
        """
        return self._client_name

    @client_name.setter
    def client_name(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("client_name must be a string")

        self._client_name = value

    @property
    def connection_timeout(self) -> _Numeric:
        """Socket timeout value in seconds for the client to connect member
        nodes.

        Setting this to ``0`` makes the connection blocking. By default, set
        to ``5.0``.
        """
        return self._connection_timeout

    @connection_timeout.setter
    def connection_timeout(self, value: _Numeric):
        if not isinstance(value, number_types):
            raise TypeError("connection_timeout must be a number")

        if value < 0:
            raise ValueError("connection_timeout must be non-negative")

        self._connection_timeout = value

    @property
    def socket_options(self) -> typing.List[typing.Tuple[int, int, typing.Union[int, bytes]]]:
        """List of socket option tuples.

        The tuples must contain the parameters passed into the
        :func:`socket.setsockopt` in the same order.
        """
        return self._socket_options

    @socket_options.setter
    def socket_options(
        self, value: typing.List[typing.Tuple[int, int, typing.Union[int, bytes]]]
    ) -> None:
        if not isinstance(value, list):
            raise TypeError("socket_options must be a list")

        for options in value:
            if not isinstance(options, tuple) or len(options) != 3:
                raise TypeError("socket_options must contain tuples of length 3 as items")

        self._socket_options = value

    @property
    def redo_operation(self) -> bool:
        """When set to ``True``, the client will redo the operations that
        were executing on the server in case if the client lost connection.

        This can happen because of network problems, or simply because the
        member died. However, it is not clear whether the operation was
        performed or not. For idempotent operations this is harmless, but for
        non-idempotent ones retrying can cause to undesirable effects. Note
        that the redo can be processed on any member. By default, set to
        ``False``.
        """
        return self._redo_operation

    @redo_operation.setter
    def redo_operation(self, value: bool) -> None:
        if not isinstance(value, bool):
            raise TypeError("redo_operation must be a boolean")

        self._redo_operation = value

    @property
    def smart_routing(self) -> bool:
        """Enables smart mode for the client instead of unisocket client.

        Smart clients send key based operations to owner of the keys.
        Unisocket clients send all operations to a single node. By default,
        set to ``True``.
        """
        return self._smart_routing

    @smart_routing.setter
    def smart_routing(self, value: bool) -> None:
        if not isinstance(value, bool):
            raise TypeError("smart_routing must be a boolean")

        self._smart_routing = value

    @property
    def ssl_enabled(self) -> bool:
        """If it is ``True``, SSL is enabled.

        By default, set to ``False``.
        """
        return self._ssl_enabled

    @ssl_enabled.setter
    def ssl_enabled(self, value: bool) -> None:
        if not isinstance(value, bool):
            raise TypeError("ssl_enabled must be a boolean")

        self._ssl_enabled = value

    @property
    def ssl_cafile(self) -> typing.Optional[str]:
        """Absolute path of concatenated CA certificates used to validate
        server's certificates in PEM format.

        When SSL is enabled and ``cafile`` is not set, a set of default CA
        certificates from default locations will be used.
        """
        return self._ssl_cafile

    @ssl_cafile.setter
    def ssl_cafile(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("ssl_cafile must be a string")

        self._ssl_cafile = value

    @property
    def ssl_certfile(self) -> typing.Optional[str]:
        """Absolute path of the client certificate in PEM format."""
        return self._ssl_certfile

    @ssl_certfile.setter
    def ssl_certfile(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("ssl_certfile must be a string")

        self._ssl_certfile = value

    @property
    def ssl_keyfile(self) -> typing.Optional[str]:
        """Absolute path of the private key file for the client certificate in
        the PEM format.

        If this parameter is ``None``, private key will be taken from the
        ``certfile``.
        """
        return self._ssl_keyfile

    @ssl_keyfile.setter
    def ssl_keyfile(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("ssl_keyfile must be a string")

        self._ssl_keyfile = value

    @property
    def ssl_password(
        self,
    ) -> typing.Optional[typing.Union[typing.Callable[[], typing.Union[str, bytes]], str, bytes]]:
        """Password for decrypting the keyfile if it is encrypted.

        The password may be a function to call to get the password. It will be
        called with no arguments, and it should return a string, bytes, or
        bytearray. If the return value is a string it will be encoded as UTF-8
        before using it to decrypt the key. Alternatively a string, bytes, or
        bytearray value may be supplied directly as the password.
        """
        return self._ssl_password

    @ssl_password.setter
    def ssl_password(
        self, value: typing.Union[typing.Callable[[], typing.Union[str, bytes]], str, bytes]
    ) -> None:
        if not isinstance(value, (str, bytes)) and not callable(value):
            raise TypeError("ssl_password must be string, bytes, or callable")

        self._ssl_password = value

    @property
    def ssl_protocol(self) -> int:
        """Protocol version used in SSL communication.

        By default, set to ``TLSv1_2``. See the
        :class:`hazelcast.config.SSLProtocol` for possible values.
        """
        return self._ssl_protocol

    @ssl_protocol.setter
    def ssl_protocol(self, value: typing.Union[str, int]) -> None:
        self._ssl_protocol = try_to_get_enum_value(value, SSLProtocol)

    @property
    def ssl_ciphers(self) -> typing.Optional[str]:
        """String in the OpenSSL cipher list format to set the available
        ciphers for sockets.

        More than one cipher can be set by separating them with a colon.
        """
        return self._ssl_ciphers

    @ssl_ciphers.setter
    def ssl_ciphers(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("ssl_ciphers must be a string")

        self._ssl_ciphers = value

    @property
    def ssl_check_hostname(self) -> bool:
        """When set to ``True``, verifies that the hostname in the member's
        certificate and the address of the member matches during the handshake.

        By default, set to ``False``.
        """
        return self._ssl_check_hostname

    @ssl_check_hostname.setter
    def ssl_check_hostname(self, value: bool) -> None:
        if not isinstance(value, bool):
            raise TypeError("ssl_check_hostname must be a boolean")

        self._ssl_check_hostname = value

    @property
    def cloud_discovery_token(self) -> typing.Optional[str]:
        """Discovery token of the Hazelcast Cloud cluster.

        When this value is set, Hazelcast Cloud discovery is enabled.
        """
        return self._cloud_discovery_token

    @cloud_discovery_token.setter
    def cloud_discovery_token(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("cloud_discovery_token must be a string")

        self._cloud_discovery_token = value

    @property
    def async_start(self) -> bool:
        """Enables non-blocking start mode of the client.

        When set to ``True``, the client creation will not wait to connect to
        cluster. The client instance will throw exceptions until it connects
        to cluster and becomes ready. If set to ``False``, the client will
        block until a cluster connection established, and it is ready to use
        the client instance. By default, set to ``False``.
        """
        return self._async_start

    @async_start.setter
    def async_start(self, value: bool) -> None:
        if not isinstance(value, bool):
            raise TypeError("async_start must be a boolean")

        self._async_start = value

    @property
    def reconnect_mode(self) -> int:
        """Defines how the client reconnects to cluster after a disconnect.

        By default, set to ``ON``. See the
        :class:`hazelcast.config.ReconnectMode` for possible values.
        """
        return self._reconnect_mode

    @reconnect_mode.setter
    def reconnect_mode(self, value: typing.Union[int, str]) -> None:
        self._reconnect_mode = try_to_get_enum_value(value, ReconnectMode)

    @property
    def retry_initial_backoff(self) -> _Numeric:
        """Wait period in seconds after the first failure before retrying.

        Must be non-negative. By default, set to ``1.0``.
        """
        return self._retry_initial_backoff

    @retry_initial_backoff.setter
    def retry_initial_backoff(self, value: _Numeric) -> None:
        if not isinstance(value, number_types):
            raise TypeError("retry_initial_backoff must be a number")

        if value < 0:
            raise ValueError("retry_initial_backoff must be non-negative")

        self._retry_initial_backoff = value

    @property
    def retry_max_backoff(self) -> _Numeric:
        """Upper bound for the backoff interval in seconds.

        Must be non-negative. By default, set to ``30.0``.
        """
        return self._retry_max_backoff

    @retry_max_backoff.setter
    def retry_max_backoff(self, value: _Numeric) -> None:
        if not isinstance(value, number_types):
            raise TypeError("retry_max_backoff must be a number")

        if value < 0:
            raise ValueError("retry_max_backoff must be non-negative")

        self._retry_max_backoff = value

    @property
    def retry_jitter(self) -> _Numeric:
        """Defines how much to randomize backoffs.

        At each iteration the calculated back-off is randomized via following
        method in pseudocode:

        ``Random(-jitter * current_backoff, jitter * current_backoff)``.

        Must be in range ``[0.0, 1.0]``. By default, set to ``0.0`` (no
        randomization).
        """
        return self._retry_jitter

    @retry_jitter.setter
    def retry_jitter(self, value: _Numeric) -> None:
        if not isinstance(value, number_types):
            raise TypeError("retry_jitter must be a number")

        if value < 0 or value > 1:
            raise ValueError("retry_jitter must be in range [0.0, 1.0]")

        self._retry_jitter = value

    @property
    def retry_multiplier(self) -> _Numeric:
        """The factor with which to multiply backoff after a failed retry.

        Must be greater than or equal to ``1``. By default, set to ``1.05``.
        """
        return self._retry_multiplier

    @retry_multiplier.setter
    def retry_multiplier(self, value: _Numeric) -> None:
        if not isinstance(value, number_types):
            raise TypeError("retry_multiplier must be a number")

        if value < 1:
            raise ValueError("retry_multiplier must be greater than or equal to 1.0")

        self._retry_multiplier = value

    @property
    def cluster_connect_timeout(self) -> _Numeric:
        """Timeout value in seconds for the client to give up connecting to
        the cluster.

        Must be non-negative or equal to `-1`. By default, set to `-1`. `-1`
        means that the client will not stop trying to the target cluster.
        (infinite timeout)
        """
        return self._cluster_connect_timeout

    @cluster_connect_timeout.setter
    def cluster_connect_timeout(self, value: _Numeric) -> None:
        if not isinstance(value, number_types):
            raise TypeError("cluster_connect_timeout must be a number")

        if value < 0 and value != _DEFAULT_CLUSTER_CONNECT_TIMEOUT:
            raise ValueError(
                "cluster_connect_timeout must be non-negative or equal to %s"
                % _DEFAULT_CLUSTER_CONNECT_TIMEOUT
            )

        self._cluster_connect_timeout = value

    @property
    def portable_version(self) -> int:
        """Default value for the portable version if the class does not have
        the :func:`get_portable_version` method.

        Portable versions are used to differentiate two versions of the
        :class:`hazelcast.serialization.api.Portable` classes that have added
        or removed fields, or fields with different types.
        """
        return self._portable_version

    @portable_version.setter
    def portable_version(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("portable_version must be a number")

        if value < 0:
            raise ValueError("portable_version must be non-negative")

        self._portable_version = value

    @property
    def data_serializable_factories(
        self,
    ) -> typing.Dict[int, typing.Dict[int, typing.Type[IdentifiedDataSerializable]]]:
        """Dictionary of factory id and corresponding
        :class:`hazelcast.serialization.api.IdentifiedDataSerializable`
        factories.

        A factory is simply a dictionary with class id and callable class
        constructors.

        .. code-block:: python

            FACTORY_ID = 1
            CLASS_ID = 1

            class SomeSerializable(IdentifiedDataSerializable):
                # omitting the implementation
                pass


            client = HazelcastClient(data_serializable_factories={
                FACTORY_ID: {
                    CLASS_ID: SomeSerializable
                }
            })
        """
        return self._data_serializable_factories

    @data_serializable_factories.setter
    def data_serializable_factories(
        self, value: typing.Dict[int, typing.Dict[int, typing.Type[IdentifiedDataSerializable]]]
    ) -> None:
        if not isinstance(value, dict):
            raise TypeError("data_serializable_factories must be a dict")

        for factory_id, factory in value.items():
            if not isinstance(factory_id, int):
                raise TypeError("Keys of data_serializable_factories must be integers")

            if not isinstance(factory, dict):
                raise TypeError("Values of data_serializable_factories must be dict")

            for class_id, clazz in factory.items():
                if not isinstance(class_id, int):
                    raise TypeError(
                        "Keys of factories of data_serializable_factories must be integers"
                    )

                if not (isinstance(clazz, type) and issubclass(clazz, IdentifiedDataSerializable)):
                    raise TypeError(
                        "Values of factories of data_serializable_factories must be "
                        "subclasses of IdentifiedDataSerializable"
                    )

        self._data_serializable_factories = value

    @property
    def portable_factories(self) -> typing.Dict[int, typing.Dict[int, typing.Type[Portable]]]:
        """Dictionary of factory id and corresponding
        :class:`hazelcast.serialization.api.Portable` factories.

        A factory is simply a dictionary with class id and callable class
        constructors.

        .. code-block:: python

            FACTORY_ID = 2
            CLASS_ID = 2

            class SomeSerializable(Portable):
                # omitting the implementation
                pass


            client = HazelcastClient(portable_factories={
                FACTORY_ID: {
                    CLASS_ID: SomeSerializable
                }
            })
        """
        return self._portable_factories

    @portable_factories.setter
    def portable_factories(
        self, value: typing.Dict[int, typing.Dict[int, typing.Type[Portable]]]
    ) -> None:
        if not isinstance(value, dict):
            raise TypeError("portable_factories must be a dict")

        for factory_id, factory in value.items():
            if not isinstance(factory_id, int):
                raise TypeError("Keys of portable_factories must be integers")

            if not isinstance(factory, dict):
                raise TypeError("Values of portable_factories must be dict")

            for class_id, clazz in factory.items():
                if not isinstance(class_id, int):
                    raise TypeError("Keys of factories of portable_factories must be integers")

                if not (isinstance(clazz, type) and issubclass(clazz, Portable)):
                    raise TypeError(
                        "Values of factories of portable_factories must be "
                        "subclasses of Portable"
                    )

        self._portable_factories = value

    @property
    def compact_serializers(self) -> typing.List[CompactSerializer]:
        """List of Compact serializers.

        .. code-block:: python

            class Foo:
                pass

            class FooSerializer(CompactSerializer[Foo]):
                pass

            client = HazelcastClient(
                compact_serializers=[
                    FooSerializer(),
                ],
            )

        """
        return self._compact_serializers

    @compact_serializers.setter
    def compact_serializers(self, value: typing.List[CompactSerializer]) -> None:
        if not isinstance(value, list):
            raise TypeError("compact_serializers must be a dict")

        for serializer in value:
            if not isinstance(serializer, CompactSerializer):
                raise TypeError("Values of compact_serializers must be CompactSerializer")

        self._compact_serializers = value

    @property
    def class_definitions(self) -> typing.List[ClassDefinition]:
        """List of all portable class definitions."""
        return self._class_definitions

    @class_definitions.setter
    def class_definitions(self, value: typing.List[ClassDefinition]) -> None:
        if not isinstance(value, list):
            raise TypeError("class_definitions must be a list")

        for cd in value:
            if not isinstance(cd, ClassDefinition):
                raise TypeError("class_definitions must contain objects of type ClassDefinition")

        self._class_definitions = value

    @property
    def check_class_definition_errors(self) -> bool:
        """When enabled, serialization system will check for class definition
        errors at start and throw an
        :class:`hazelcast.errors.HazelcastSerializationError` with error
        definition.

        By default, set to ``True``.
        """
        return self._check_class_definition_errors

    @check_class_definition_errors.setter
    def check_class_definition_errors(self, value: bool) -> None:
        if not isinstance(value, bool):
            raise TypeError("check_class_definition_errors must be a boolean")

        self._check_class_definition_errors = value

    @property
    def is_big_endian(self) -> bool:
        """Defines if big-endian is used as the byte order for the
        serialization.

        By default, set to ``True``.
        """
        return self._is_big_endian

    @is_big_endian.setter
    def is_big_endian(self, value: bool) -> None:
        if not isinstance(value, bool):
            raise TypeError("is_big_endian must be a boolean")

        self._is_big_endian = value

    @property
    def default_int_type(self) -> int:
        """Defines how the ``int`` type is represented on the member side.

        By default, it is serialized as ``INT`` (``32`` bits). See the
        :class:`hazelcast.config.IntType` for possible values.
        """
        return self._default_int_type

    @default_int_type.setter
    def default_int_type(self, value: typing.Union[int, str]) -> None:
        self._default_int_type = try_to_get_enum_value(value, IntType)

    @property
    def global_serializer(self) -> typing.Optional[typing.Type[StreamSerializer]]:
        """Defines the global serializer.

        This serializer is registered as a fallback serializer to handle all
        other objects if a serializer cannot be located for them.
        """
        return self._global_serializer

    @global_serializer.setter
    def global_serializer(self, value: typing.Type[StreamSerializer]) -> None:
        if not isinstance(value, type) or not issubclass(value, StreamSerializer):
            raise TypeError("global_serializer must be a StreamSerializer")

        self._global_serializer = value

    @property
    def custom_serializers(
        self,
    ) -> typing.Dict[typing.Type[typing.Any], typing.Type[StreamSerializer]]:
        """Dictionary of class and the corresponding custom serializers.

        .. code-block:: python

            class SomeClass:
                # omitting the implementation
                pass


            class SomeClassSerializer(StreamSerializer):
                # omitting the implementation
                pass

            client = HazelcastClient(custom_serializers={
                SomeClass: SomeClassSerializer
            })
        """
        return self._custom_serializers

    @custom_serializers.setter
    def custom_serializers(
        self, value: typing.Dict[typing.Type[typing.Any], typing.Type[StreamSerializer]]
    ) -> None:
        if not isinstance(value, dict):
            raise TypeError("custom_serializers must be a dict")

        for _type, serializer in value.items():
            if not isinstance(_type, type):
                raise TypeError("Keys of custom_serializers must be types")

            if not (isinstance(serializer, type) and issubclass(serializer, StreamSerializer)):
                raise TypeError(
                    "Values of custom_serializers must be subclasses of StreamSerializer"
                )

        self._custom_serializers = value

    @property
    def near_caches(self) -> typing.Dict[str, "NearCacheConfig"]:
        """Dictionary of near cache names to the corresponding near cache
        configurations.

        See the :class:`hazelcast.config.NearCacheConfig` for the possible
        configuration options.

        The near cache configuration can also be passed as a dictionary of
        configuration option name to value. When an option is missing from the
        dictionary configuration, it will be set to its default value.
        """
        return self._near_caches

    @near_caches.setter
    def near_caches(self, value: typing.Dict[str, "NearCacheConfig"]) -> None:
        if not isinstance(value, dict):
            raise TypeError("near_caches must be a dict")

        configs = dict()
        for name, config in value.items():
            if not isinstance(name, str):
                raise TypeError("Keys of near_caches must be strings")

            if not isinstance(config, (dict, NearCacheConfig)):
                raise TypeError("Values of near_caches must be a NearCacheConfig or a dict")

            if isinstance(config, dict):
                config = NearCacheConfig.from_dict(config)

            configs[name] = config

        self._near_caches = configs

    @property
    def load_balancer(self) -> typing.Optional[LoadBalancer]:
        """Load balancer implementation for the client."""
        return self._load_balancer

    @load_balancer.setter
    def load_balancer(self, value: LoadBalancer) -> None:
        if not isinstance(value, LoadBalancer):
            raise TypeError("load_balancer must be a LoadBalancer")

        self._load_balancer = value

    @property
    def membership_listeners(
        self,
    ) -> typing.List[typing.Tuple[_MembershipListenerType, _MembershipListenerType]]:
        """List of membership listener tuples.

        Tuples must be of size ``2``. The first element will be the function
        to be called when a member is added, and the second element will be
        the function to be called when the member is removed with the
        :class:`hazelcast.core.MemberInfo` as the only parameter.

        Any of the elements can be ``None``, but not at the same time.
        """
        return self._membership_listeners

    @membership_listeners.setter
    def membership_listeners(
        self, value: typing.List[typing.Tuple[_MembershipListenerType, _MembershipListenerType]]
    ) -> None:
        if not isinstance(value, list):
            raise TypeError("membership_listeners must be a list")

        for listener in value:
            if not isinstance(listener, tuple) or len(listener) != 2:
                raise TypeError("membership_listeners must contain tuples of length 2 as items")

            added, removed = listener

            if not (callable(added) or callable(removed)):
                raise TypeError("At least one of the listeners in the tuple most be callable")

        self._membership_listeners = value

    @property
    def lifecycle_listeners(self) -> typing.List[typing.Callable[[str], None]]:
        """List of lifecycle listeners.

        Listeners will be called with the new lifecycle state as the only
        parameter when the client changes lifecycle states.
        """
        return self._lifecycle_listeners

    @lifecycle_listeners.setter
    def lifecycle_listeners(self, value: typing.List[typing.Callable[[str], None]]) -> None:
        if not isinstance(value, list):
            raise TypeError("lifecycle_listeners must be a list")

        for listener in value:
            if not callable(listener):
                raise TypeError("lifecycle_listeners must contain callable items")

        self._lifecycle_listeners = value

    @property
    def flake_id_generators(self) -> typing.Dict[str, "FlakeIdGeneratorConfig"]:
        """Dictionary of flake id generator names to the corresponding flake
        id generator configurations.

        See the :class:`hazelcast.config.FlakeIdGeneratorConfig` for the
        possible configuration options.

        The flake id generator configuration can also be passed as a
        dictionary of configuration option name to value. When an option is
        missing from the dictionary configuration, it will be set to its
        default value.
        """
        return self._flake_id_generators

    @flake_id_generators.setter
    def flake_id_generators(self, value: typing.Dict[str, "FlakeIdGeneratorConfig"]) -> None:
        if not isinstance(value, dict):
            raise TypeError("flake_id_generators must be a dict")

        configs = dict()
        for name, config in value.items():
            if not isinstance(name, str):
                raise TypeError("Keys of flake_id_generators must be strings")

            if not isinstance(config, (dict, FlakeIdGeneratorConfig)):
                raise TypeError(
                    "Values of flake_id_generators must be a FlakeIdGeneratorConfig or a dict"
                )

            if isinstance(config, dict):
                config = FlakeIdGeneratorConfig.from_dict(config)

            configs[name] = config

        self._flake_id_generators = configs

    @property
    def reliable_topics(self) -> typing.Dict[str, "ReliableTopicConfig"]:
        """Dictionary of reliable topic names to the corresponding reliable
        topic configurations.

        See the :class:`hazelcast.config.ReliableTopicConfig` for the
        possible configuration options.

        The reliable topic configuration can also be passed as a dictionary of
        configuration option name to value. When an option is missing from the
        dictionary configuration, it will be set to its default value.
        """
        return self._reliable_topics

    @reliable_topics.setter
    def reliable_topics(self, value: typing.Dict[str, "ReliableTopicConfig"]) -> None:
        if not isinstance(value, dict):
            raise TypeError("reliable_topics must be a dict")

        configs = {}
        for name, config in value.items():
            if not isinstance(name, str):
                raise TypeError("Keys of reliable_topics must be strings")

            if not isinstance(config, (dict, ReliableTopicConfig)):
                raise TypeError("Values of reliable_topics must be a ReliableTopicConfig or a dict")

            if isinstance(config, dict):
                config = ReliableTopicConfig.from_dict(config)

            configs[name] = config

        self._reliable_topics = configs

    @property
    def labels(self) -> typing.List[str]:
        """Labels for the client to be sent to the cluster."""
        return self._labels

    @labels.setter
    def labels(self, value: typing.List[str]) -> None:
        if not isinstance(value, list):
            raise TypeError("labels must be a list")

        for label in value:
            if not isinstance(label, str):
                raise TypeError("labels must be list of strings")

        self._labels = value

    @property
    def heartbeat_interval(self) -> _Numeric:
        """Time interval between the heartbeats sent by the client to the
        member nodes in seconds.

        By default, set to ``5.0``.
        """
        return self._heartbeat_interval

    @heartbeat_interval.setter
    def heartbeat_interval(self, value: _Numeric) -> None:
        if not isinstance(value, number_types):
            raise TypeError("heartbeat_interval must be a number")

        if value <= 0:
            raise ValueError("heartbeat_interval must be positive")

        self._heartbeat_interval = value

    @property
    def heartbeat_timeout(self) -> _Numeric:
        """If there is no message passing between the client and a member
        within the given time via this property in seconds, the connection
        will be closed.

        By default, set to ``60.0``.
        """
        return self._heartbeat_timeout

    @heartbeat_timeout.setter
    def heartbeat_timeout(self, value: _Numeric) -> None:
        if not isinstance(value, number_types):
            raise TypeError("heartbeat_timeout must be a number")

        if value <= 0:
            raise ValueError("heartbeat_timeout must be positive")

        self._heartbeat_timeout = value

    @property
    def invocation_timeout(self) -> _Numeric:
        """When an invocation gets an exception because

        - Member throws an exception.
        - Connection between the client and member is closed.
        - The client's heartbeat requests are timed out.

        Time passed since invocation started is compared with this property.
        If the time is already passed, then the exception is delegated to the
        user. If not, the invocation is retried. Note that, if invocation gets
        no exception, and it is a long-running one, then it will not get any
        exception, no matter how small this timeout is set. Time unit is in
        seconds.

        By default, set to ``120.0``.
        """
        return self._invocation_timeout

    @invocation_timeout.setter
    def invocation_timeout(self, value: _Numeric) -> None:
        if not isinstance(value, number_types):
            raise TypeError("invocation_timeout must be a number")

        if value <= 0:
            raise ValueError("invocation_timeout must be positive")

        self._invocation_timeout = value

    @property
    def invocation_retry_pause(self) -> _Numeric:
        """Pause time between each retry cycle of an invocation in seconds.

        By default, set to ``1.0``.
        """
        return self._invocation_retry_pause

    @invocation_retry_pause.setter
    def invocation_retry_pause(self, value: _Numeric) -> None:
        if not isinstance(value, number_types):
            raise TypeError("invocation_retry_pause must be a number")

        if value <= 0:
            raise ValueError("invocation_retry_pause must be positive")

        self._invocation_retry_pause = value

    @property
    def statistics_enabled(self) -> bool:
        """When set to ``True``, the client statistics collection is enabled.

        By default, set to ``False``.
        """
        return self._statistics_enabled

    @statistics_enabled.setter
    def statistics_enabled(self, value: bool) -> None:
        if not isinstance(value, bool):
            raise TypeError("statistics_enabled must be a boolean")

        self._statistics_enabled = value

    @property
    def statistics_period(self) -> _Numeric:
        """The period in seconds the statistics run."""
        return self._statistics_period

    @statistics_period.setter
    def statistics_period(self, value: _Numeric) -> None:
        if not isinstance(value, number_types):
            raise TypeError("statistics_period must be a number")

        if value <= 0:
            raise ValueError("statistics_period must be positive")

        self._statistics_period = value

    @property
    def shuffle_member_list(self) -> bool:
        """The client shuffles the given member list to prevent all clients to
        connect to the same node when this property is set to ``True``.

        When it is set to ``False``, the client tries to connect to the nodes
        in the given order.

        By default, set to ``True``.
        """
        return self._shuffle_member_list

    @shuffle_member_list.setter
    def shuffle_member_list(self, value: bool) -> None:
        if not isinstance(value, bool):
            raise TypeError("shuffle_member_list must be a boolean")

        self._shuffle_member_list = value

    @property
    def backup_ack_to_client_enabled(self) -> bool:
        """Enables the client to get backup acknowledgements directly from the
        member that backups are applied, which reduces number of hops and
        increases performance for smart clients.

        This option has no effect for unisocket clients.

        By default, set to ``True`` (enabled).
        """
        return self._backup_ack_to_client_enabled

    @backup_ack_to_client_enabled.setter
    def backup_ack_to_client_enabled(self, value: bool) -> None:
        if not isinstance(value, bool):
            raise TypeError("backup_ack_to_client_enabled must be a boolean")

        self._backup_ack_to_client_enabled = value

    @property
    def operation_backup_timeout(self) -> _Numeric:
        """If an operation has backups, defines how long the invocation will
        wait for acks from the backup replicas in seconds.

        If acks are not received from some backups, there won't be any
        rollback on other successful replicas.

        By default, set to ``5.0``.
        """
        return self._operation_backup_timeout

    @operation_backup_timeout.setter
    def operation_backup_timeout(self, value: _Numeric) -> None:
        if not isinstance(value, number_types):
            raise TypeError("operation_backup_timeout must be a number")

        if value <= 0:
            raise ValueError("operation_backup_timeout must be positive")

        self._operation_backup_timeout = value

    @property
    def fail_on_indeterminate_operation_state(self) -> bool:
        """When enabled, if an operation has sync backups and acks are not
        received from backup replicas in time, or the member which owns
        primary replica of the target partition leaves the cluster, then the
        invocation fails with
        :class:`hazelcast.errors.IndeterminateOperationStateError`.

        However, even if the invocation fails, there will not be any rollback
        on other successful replicas.

        By default, set to ``False`` (do not fail).
        """
        return self._fail_on_indeterminate_operation_state

    @fail_on_indeterminate_operation_state.setter
    def fail_on_indeterminate_operation_state(self, value: bool) -> None:
        if not isinstance(value, bool):
            raise TypeError("fail_on_indeterminate_operation_state must be a boolean")

        self._fail_on_indeterminate_operation_state = value

    @property
    def creds_username(self) -> typing.Optional[str]:
        """Username for credentials authentication (Enterprise feature)."""
        return self._creds_username

    @creds_username.setter
    def creds_username(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("creds_password must be a string")

        self._creds_username = value

    @property
    def creds_password(self) -> typing.Optional[str]:
        """Password for credentials authentication (Enterprise feature)."""
        return self._creds_password

    @creds_password.setter
    def creds_password(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("creds_password must be a string")

        self._creds_password = value

    @property
    def token_provider(self) -> typing.Optional[TokenProvider]:
        """Token provider for custom authentication (Enterprise feature).

        Note that ``token_provider`` setting has priority over credentials
        settings.
        """
        return self._token_provider

    @token_provider.setter
    def token_provider(self, value: TokenProvider) -> None:
        token_fun = getattr(value, "token", None)
        if token_fun is None or not isinstance(token_fun, types.MethodType):
            raise TypeError("token_provider must be an object with a token method")

        self._token_provider = value

    @property
    def use_public_ip(self) -> bool:
        """When set to ``True``, the client uses the public IP addresses
        reported by members while connecting to them, if available.

        By default, set to ``False``.
        """
        return self._use_public_ip

    @use_public_ip.setter
    def use_public_ip(self, value: bool) -> None:
        if not isinstance(value, bool):
            raise TypeError("use_public_ip must be a boolean")

        self._use_public_ip = value

    @classmethod
    def from_dict(cls, d: typing.Dict[str, typing.Any]) -> "Config":
        """Constructs a configuration object out of the given dictionary.

        The dictionary items must be valid pairs of configuration option name
        to its value.

        If a configuration is missing from the dictionary, the default value
        for it will be used.

        Args:
            d: Dictionary that describes the configuration.

        Returns:
            The constructed configuration object.
        """
        config = cls()
        for k, v in d.items():
            if v is not None:
                try:
                    setattr(config, k, v)
                except AttributeError:
                    raise InvalidConfigurationError("Unrecognized config option: %s" % k)
        return config


class NearCacheConfig:
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
        self._invalidate_on_change: bool = True
        self._in_memory_format: int = InMemoryFormat.BINARY
        self._time_to_live: typing.Optional[_Numeric] = None
        self._max_idle: typing.Optional[_Numeric] = None
        self._eviction_policy: int = EvictionPolicy.LRU
        self._eviction_max_size: int = 10000
        self._eviction_sampling_count: int = 8
        self._eviction_sampling_pool_size: int = 16

    @property
    def invalidate_on_change(self) -> bool:
        """Enables cluster-assisted invalidate on change behavior.

        When set to ``True``, entries are invalidated when they are changed in
        cluster.

        By default, set to ``True``.
        """
        return self._invalidate_on_change

    @invalidate_on_change.setter
    def invalidate_on_change(self, value: bool) -> None:
        if not isinstance(value, bool):
            raise TypeError("invalidate_on_change must be a boolean")

        self._invalidate_on_change = value

    @property
    def in_memory_format(self) -> int:
        """Specifies in which format data will be stored in the Near Cache.

        See the :class:`hazelcast.config.InMemoryFormat` for possible values.

        By default, set to ``BINARY``.
        """
        return self._in_memory_format

    @in_memory_format.setter
    def in_memory_format(self, value: typing.Union[int, str]) -> None:
        self._in_memory_format = try_to_get_enum_value(value, InMemoryFormat)

    @property
    def time_to_live(self) -> typing.Optional[_Numeric]:
        """Maximum number of seconds that an entry can stay in cache.

        When not set, entries won't be evicted due to expiration.
        """
        return self._time_to_live

    @time_to_live.setter
    def time_to_live(self, value: _Numeric) -> None:
        if not isinstance(value, number_types):
            raise TypeError("time_to_live must be a number")

        if value < 0:
            raise ValueError("time_to_live must be non-negative")

        self._time_to_live = value

    @property
    def max_idle(self) -> typing.Optional[_Numeric]:
        """Maximum number of seconds that an entry can stay in the Near Cache
        until it is accessed.

        When not set, entries won't be evicted due to inactivity.
        """
        return self._max_idle

    @max_idle.setter
    def max_idle(self, value: _Numeric) -> None:
        if not isinstance(value, number_types):
            raise TypeError("max_idle must be a number")

        if value < 0:
            raise ValueError("max_idle must be non-negative")

        self._max_idle = value

    @property
    def eviction_policy(self) -> int:
        """Defines eviction policy configuration.

        See the:class:`hazelcast.config.EvictionPolicy` for possible values.

        By default, set to ``LRU``.
        """
        return self._eviction_policy

    @eviction_policy.setter
    def eviction_policy(self, value: typing.Union[int, str]) -> None:
        self._eviction_policy = try_to_get_enum_value(value, EvictionPolicy)

    @property
    def eviction_max_size(self) -> int:
        """Defines maximum number of entries kept in the memory before
        eviction kicks in.

        By default, set to ``10000``.
        """
        return self._eviction_max_size

    @eviction_max_size.setter
    def eviction_max_size(self, value: int):
        if not isinstance(value, int):
            raise TypeError("eviction_max_size must be an int")

        if value < 1:
            raise ValueError("eviction_max_size must be greater than 1")

        self._eviction_max_size = value

    @property
    def eviction_sampling_count(self) -> int:
        """Number of random entries that are evaluated to see if some of them
        are already expired.

        By default, set to ``8``.
        """
        return self._eviction_sampling_count

    @eviction_sampling_count.setter
    def eviction_sampling_count(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("eviction_sampling_count must be an int")

        if value < 1:
            raise ValueError("eviction_sampling_count must be greater than 1")

        self._eviction_sampling_count = value

    @property
    def eviction_sampling_pool_size(self) -> int:
        """Size of the pool for eviction candidates.

        The pool is kept sorted  according to the eviction policy. By default,
        set to ``16``.
        """
        return self._eviction_sampling_pool_size

    @eviction_sampling_pool_size.setter
    def eviction_sampling_pool_size(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("eviction_sampling_pool_size must be an int")

        if value < 1:
            raise ValueError("eviction_sampling_pool_size must be greater than 1")

        self._eviction_sampling_pool_size = value

    @classmethod
    def from_dict(cls, d: typing.Dict[str, typing.Any]) -> "NearCacheConfig":
        """Constructs a configuration object out of the given dictionary.

        The dictionary items must be valid pairs of configuration option name
        to its value.

        If a configuration is missing from the dictionary, the default value
        for it will be used.

        Args:
            d: Dictionary that describes the configuration.

        Returns:
            The constructed configuration object.
        """
        config = cls()
        for k, v in d.items():
            try:
                setattr(config, k, v)
            except AttributeError:
                raise InvalidConfigurationError(
                    "Unrecognized config option for the near cache: %s" % k
                )
        return config


class FlakeIdGeneratorConfig:
    __slots__ = ("_prefetch_count", "_prefetch_validity")

    def __init__(self):
        self._prefetch_count = 100
        self._prefetch_validity = 600

    @property
    def prefetch_count(self) -> int:
        """Defines how many IDs are pre-fetched on the background when a new
        flake id is requested from the cluster.

        Should be in the range ``1..100000``. By default, set to ``100``.
        """
        return self._prefetch_count

    @prefetch_count.setter
    def prefetch_count(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("prefetch_count must be an int")

        if not (0 < value <= 100000):
            raise ValueError("prefetch_count must be in range 1 to 100000")

        self._prefetch_count = value

    @property
    def prefetch_validity(self) -> _Numeric:
        """Defines for how long the pre-fetched IDs can be used.

        If this time elapsed, a new batch of IDs will be fetched. Time unit is
        in seconds. By default, set to ``600`` (10 minutes).

        The IDs contain timestamp component, which ensures rough global
        ordering of IDs. If an ID is assigned to an object that was created
        much later, it will be much out of order. If you don't care about
        ordering, set this value to ``0`` for unlimited ID validity.
        """
        return self._prefetch_validity

    @prefetch_validity.setter
    def prefetch_validity(self, value: _Numeric):
        if not isinstance(value, number_types):
            raise TypeError("prefetch_validity must be a number")

        if value < 0:
            raise ValueError("prefetch_validity must be non-negative")

        self._prefetch_validity = value

    @classmethod
    def from_dict(cls, d: typing.Dict[str, typing.Any]) -> "FlakeIdGeneratorConfig":
        """Constructs a configuration object out of the given dictionary.

        The dictionary items must be valid pairs of configuration option name
        to its value.

        If a configuration is missing from the dictionary, the default value
        for it will be used.

        Args:
            d: Dictionary that describes the configuration.

        Returns:
            The constructed configuration object.
        """
        config = cls()
        for k, v in d.items():
            try:
                setattr(config, k, v)
            except AttributeError:
                raise InvalidConfigurationError(
                    "Unrecognized config option for the flake id generator: %s" % k
                )
        return config


class ReliableTopicConfig:
    __slots__ = ("_read_batch_size", "_overload_policy")

    def __init__(self):
        self._read_batch_size: int = 10
        self._overload_policy: int = TopicOverloadPolicy.BLOCK

    @property
    def read_batch_size(self) -> int:
        """Number of messages the reliable topic will try to read in batch.

        It will get at least one, but if there are more available, then it
        will try to get more to increase throughput. By default, set to ``10``.
        """
        return self._read_batch_size

    @read_batch_size.setter
    def read_batch_size(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("read_batch_size must be an int")

        if value <= 0:
            raise ValueError("read_batch_size must be positive")

        self._read_batch_size = value

    @property
    def overload_policy(self) -> int:
        """Policy to handle an overloaded topic.

        By default, set to ``BLOCK``. See the
        :class:`hazelcast.config.TopicOverloadPolicy` for possible values.
        """
        return self._overload_policy

    @overload_policy.setter
    def overload_policy(self, value: typing.Union[int, str]) -> None:
        self._overload_policy = try_to_get_enum_value(value, TopicOverloadPolicy)

    @classmethod
    def from_dict(cls, d: typing.Dict[str, typing.Any]) -> "ReliableTopicConfig":
        """Constructs a configuration object out of the given dictionary.

        The dictionary items must be valid pairs of configuration option name
        to its value.

        If a configuration is missing from the dictionary, the default value
        for it will be used.

        Args:
            d: Dictionary that describes the configuration.

        Returns:
            The constructed configuration object.
        """
        config = cls()
        for k, v in d.items():
            try:
                setattr(config, k, v)
            except AttributeError:
                raise InvalidConfigurationError(
                    "Unrecognized config option for the reliable topic: %s" % k
                )
        return config


class BitmapIndexOptions:
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
        for k, v in d.items():
            try:
                setattr(options, k, v)
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


class IndexConfig:
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
        if not isinstance(value, str) and value is not None:
            raise TypeError("name must be a string or None")

        self._name = value

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
        if not isinstance(value, list):
            raise TypeError("attributes must be a list")

        for attribute in value:
            IndexUtil.validate_attribute(attribute)

        self._attributes = value

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
        for k, v in d.items():
            if v is not None:
                try:
                    setattr(config, k, v)
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


class IndexUtil:
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
