"""Hazelcast Core objects and constants."""
import json


CLIENT_TYPE = "PYH"
SERIALIZATION_VERSION = 1


class MemberInfo(object):
    """
    Represents a member in the cluster with its address, uuid, lite member
    status, attributes, version, and address map.
    """

    __slots__ = ("address", "uuid", "attributes", "lite_member", "version", "address_map")

    def __init__(self, address, uuid, attributes, lite_member, version, _, address_map):
        self.address = address
        """
        Address: Address of the member.
        """

        self.uuid = uuid
        """
        uuid.UUID: UUID of the member.
        """

        self.attributes = attributes
        """
        dict[str, str]: Configured attributes of the member.
        """

        self.lite_member = lite_member
        """
        bool: ``True`` if the member is a lite member, ``False`` otherwise.
        Lite members do not own any partition.
        """

        self.version = version
        """
        MemberVersion: Hazelcast codebase version of the member.
        """

        self.address_map = address_map
        """
        dict[EndpointQualifier, Address]: Dictionary of server socket
        addresses per :class:`EndpointQualifier` of this member.
        """

    def __str__(self):
        return "Member [%s]:%s - %s%s" % (
            self.address.host,
            self.address.port,
            self.uuid,
            " lite" if self.lite_member else "",
        )

    def __repr__(self):
        return "Member(address=%s, uuid=%s, attributes=%s, lite_member=%s, version=%s)" % (
            self.address,
            self.uuid,
            self.attributes,
            self.lite_member,
            self.version,
        )

    def __hash__(self):
        return hash((self.address, self.uuid))

    def __eq__(self, other):
        return (
            isinstance(other, MemberInfo)
            and self.address == other.address
            and self.uuid == other.uuid
        )

    def __ne__(self, other):
        return not self.__eq__(other)


class Address(object):
    """Represents an address of a member in the cluster."""

    def __init__(self, host, port):
        self.host = host
        """
        str: Host of the address.
        """

        self.port = port
        """
        int: Port of the address.
        """

    def __repr__(self):
        return "Address(host=%s, port=%d)" % (self.host, self.port)

    def __hash__(self):
        return hash((self.host, self.port))

    def __eq__(self, other):
        return isinstance(other, Address) and self.host == other.host and self.port == other.port

    def __ne__(self, other):
        return not self.__eq__(other)


class ProtocolType(object):
    """Types of server sockets.

    A member typically responds to several types of protocols for
    member-to-member, client-member protocol, WAN communication etc. The
    default configuration uses a single server socket to listen for all kinds
    of protocol types configured, while Advanced Network Config of the server
    allows configuration of multiple server sockets.
    """

    # We had to put dummy documentations for the constants
    # so that they are displayed on the API documentation.

    MEMBER = 0
    """Type of member server sockets."""

    CLIENT = 1
    """Type of client server sockets."""

    WAN = 2
    """Type of WAN server sockets."""

    REST = 3
    """Type of REST server sockets."""

    MEMCACHE = 4
    """Type of Memcached server sockets."""


class EndpointQualifier(object):
    """Uniquely identifies groups of network connections sharing a common
    :class:`ProtocolType` and the same network settings, when Hazelcast server
    is configured with Advanced Network Configuration enabled.

    In some cases, just the :class:`ProtocolType` is enough (e.g. since there
    can be only a single member server socket).

    When just the :class:`ProtocolType` is not enough (for example when
    configuring outgoing WAN connections to 2 different target clusters),
    an :attr:`identifier` is used to uniquely identify the network
    configuration.
    """

    __slots__ = ("_protocol_type", "_identifier")

    def __init__(self, protocol_type, identifier):
        self._protocol_type = protocol_type
        self._identifier = identifier

    @property
    def protocol_type(self):
        """ProtocolType: Protocol type of the endpoint."""
        return self._protocol_type

    @property
    def identifier(self):
        """str: Unique identifier for same-protocol-type endpoints."""
        return self._identifier

    def __eq__(self, other):
        return (
            isinstance(other, EndpointQualifier)
            and self._protocol_type == other._protocol_type
            and self._identifier == other._identifier
        )

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash((self._protocol_type, self._identifier))

    def __repr__(self):
        return "EndpointQualifier(protocol_type=%s, identifier=%s)" % (
            self._protocol_type,
            self._identifier,
        )


class AddressHelper(object):
    @staticmethod
    def get_possible_addresses(address):
        address = AddressHelper.address_from_str(address)
        possible_port = address.port
        port_try_count = 1
        if possible_port == -1:
            port_try_count = 3
            possible_port = 5701

        addresses = []
        for i in range(port_try_count):
            addresses.append(Address(address.host, possible_port + i))

        # primary, secondary
        return [addresses.pop(0)], addresses

    @staticmethod
    def address_from_str(address, port=-1):
        bracket_start_idx = address.find("[")
        bracket_end_idx = address.find("]", bracket_start_idx)
        colon_idx = address.find(":")
        last_colon_idx = address.rfind(":")

        if -1 < colon_idx < last_colon_idx:
            # IPv6
            if bracket_start_idx == 0 and bracket_end_idx > bracket_start_idx:
                host = address[bracket_start_idx + 1 : bracket_end_idx]
                if last_colon_idx == (bracket_end_idx + 1):
                    port = int(address[last_colon_idx + 1 :])
            else:
                host = address
        elif colon_idx > 0 and colon_idx == last_colon_idx:
            host = address[:colon_idx]
            port = int(address[colon_idx + 1 :])
        else:
            host = address
        return Address(host, port)


class DistributedObjectInfo(object):
    def __init__(self, service_name, name):
        self.service_name = service_name
        self.name = name

    def __repr__(self):
        return "DistributedObjectInfo(serviceName=%s, name=%s)" % (self.service_name, self.name)

    def __hash__(self):
        return hash((self.name, self.service_name))

    def __eq__(self, other):
        if isinstance(other, DistributedObjectInfo):
            return self.name == other.name and self.service_name == other.service_name
        return False


class DistributedObjectEventType(object):
    """Type of the distributed object event."""

    CREATED = "CREATED"
    """
    DistributedObject is created.
    """

    DESTROYED = "DESTROYED"
    """
    DistributedObject is destroyed.
    """


class DistributedObjectEvent(object):
    """Distributed Object Event"""

    def __init__(self, name, service_name, event_type, source):
        self.name = name
        """
        str: Name of the distributed object.
        """

        self.service_name = service_name
        """
        str: Service name of the distributed object.
        """

        self.event_type = event_type
        """
        str: Event type. Either ``CREATED`` or ``DESTROYED``.
        """

        self.source = source
        """
        uuid.UUID: UUID of the member that fired the event.
        """

    def __repr__(self):
        return "DistributedObjectEvent(name=%s, service_name=%s, event_type=%s, source=%s)" % (
            self.name,
            self.service_name,
            self.event_type,
            self.source,
        )


class SimpleEntryView(object):
    """EntryView represents a readonly view of a map entry."""

    def __init__(
        self,
        key,
        value,
        cost,
        creation_time,
        expiration_time,
        hits,
        last_access_time,
        last_stored_time,
        last_update_time,
        version,
        ttl,
        max_idle,
    ):
        self.key = key
        """
        The key of the entry.
        """

        self.value = value
        """
        The value of the entry.
        """

        self.cost = cost
        """
        int: The cost in bytes of the entry.
        """

        self.creation_time = creation_time
        """
        int: The creation time of the entry.
        """

        self.expiration_time = expiration_time
        """
        int: The expiration time of the entry.
        """

        self.hits = hits
        """
        int: Number of hits of the entry.
        """

        self.last_access_time = last_access_time
        """
        int: The last access time for the entry.
        """

        self.last_stored_time = last_stored_time
        """
        int: The last store time for the value.
        """

        self.last_update_time = last_update_time
        """
        int: The last time the value was updated.
        """

        self.version = version
        """
        int: The version of the entry.
        """

        self.ttl = ttl
        """
        int: The last set time to live milliseconds.
        """

        self.max_idle = max_idle
        """
        int: The last set max idle time in milliseconds.
        """

    def __repr__(self):
        return (
            "SimpleEntryView(key=%s, value=%s, cost=%s, creation_time=%s, "
            "expiration_time=%s, hits=%s, last_access_time=%s, last_stored_time=%s, "
            "last_update_time=%s, version=%s, ttl=%s, max_idle=%s"
            % (
                self.key,
                self.value,
                self.cost,
                self.creation_time,
                self.expiration_time,
                self.hits,
                self.last_access_time,
                self.last_stored_time,
                self.last_update_time,
                self.version,
                self.ttl,
                self.max_idle,
            )
        )


class HazelcastJsonValue(object):
    """HazelcastJsonValue is a wrapper for JSON formatted strings.

    It is preferred to store HazelcastJsonValue instead of Strings for JSON formatted strings.
    Users can run predicates and use indexes on the attributes of the underlying
    JSON strings.

    HazelcastJsonValue is queried using Hazelcast's querying language.

    In terms of querying, numbers in JSON strings are treated as either
    Long or Double in the Java side. str, bool and None
    are treated as String, boolean and null respectively.

    HazelcastJsonValue keeps given string as it is. Strings are not
    checked for being valid. Ill-formatted JSON strings may cause false
    positive or false negative results in queries.

    HazelcastJsonValue can also be constructed from JSON serializable objects.
    In that case, objects are converted to JSON strings and stored as such.
    If an error occurs during the conversion, it is raised directly.

    None values are not allowed.
    """

    def __init__(self, value):
        if value is None:
            raise AssertionError("JSON string or the object cannot be None.")
        if isinstance(value, str):
            self._json_string = value
        else:
            self._json_string = json.dumps(value)

    def to_string(self):
        """Returns unaltered string that was used to create this object.

        Returns:
            str: The original string.
        """
        return self._json_string

    def loads(self):
        """Deserializes the string that was used to create this object
        and returns as Python object.

        Returns:
            any: The Python object represented by the original string.
        """
        return json.loads(self._json_string)

    def __eq__(self, other):
        return isinstance(other, HazelcastJsonValue) and self._json_string == other._json_string

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self._json_string)

    def __repr__(self):
        return self._json_string


class MemberVersion(object):
    """
    Determines the Hazelcast codebase version in terms of major.minor.patch version.
    """

    __slots__ = ("major", "minor", "patch")

    def __init__(self, major, minor, patch):
        self.major = major
        self.minor = minor
        self.patch = patch

    def __repr__(self):
        return "MemberVersion(major=%s, minor=%s, patch=%s)" % (self.major, self.minor, self.patch)


class MapEntry(object):
    """
    Represents the entry of a Map, with key and value fields.
    """

    __slots__ = ("_key", "_value")

    def __init__(self, key=None, value=None):
        self._key = key
        self._value = value

    @property
    def key(self):
        """Key of the entry."""
        return self._key

    @property
    def value(self):
        """Value of the entry."""
        return self._value
