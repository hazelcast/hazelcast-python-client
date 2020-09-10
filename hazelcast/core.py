"""Hazelcast Core objects"""
import json

from hazelcast import six
from hazelcast import util
from hazelcast.util import with_reserved_items


class MemberInfo(object):
    __slots__ = ("address", "uuid", "attributes", "lite_member", "version")

    """
    Represents a member in the cluster with its address, uuid, lite member status, attributes and version.
    """

    def __init__(self, address, uuid, attributes, lite_member, version, *_):
        self.address = address
        self.uuid = uuid
        self.attributes = attributes
        self.lite_member = lite_member
        self.version = version

    def __str__(self):
        return "Member [%s]:%s - %s" % (self.address.host, self.address.port, self.uuid)

    def __repr__(self):
        return "Member(address=%s, uuid=%s, attributes=%s, lite_member=%s, version=%s)" \
               % (self.address, self.uuid, self.attributes, self.lite_member, self.version)

    def __hash__(self):
        return hash((self.address, self.uuid))

    def __eq__(self, other):
        return isinstance(other, MemberInfo) and self.address == other.address and self.uuid == other.uuid

    def __ne__(self, other):
        return not self.__eq__(other)


class Address(object):
    """
    Represents an address of a member in the cluster.
    """

    def __init__(self, host, port):
        self.host = host
        self.port = port

    def __repr__(self):
        return "Address(host=%s, port=%d)" % (self.host, self.port)

    def __hash__(self):
        return hash((self.host, self.port))

    def __eq__(self, other):
        return isinstance(other, Address) and self.host == other.host and self.port == other.port

    def __ne__(self, other):
        return not self.__eq__(other)


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
                host = address[bracket_start_idx + 1: bracket_end_idx]
                if last_colon_idx == (bracket_end_idx + 1):
                    port = int(address[last_colon_idx + 1:])
            else:
                host = address
        elif colon_idx > 0 and colon_idx == last_colon_idx:
            host = address[:colon_idx]
            port = int(address[colon_idx + 1:])
        else:
            host = address
        return Address(host, port)


class DistributedObjectInfo(object):
    """
    Represents name of the Distributed Object and the name of service which it belongs to.
    """

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


@with_reserved_items
class DistributedObjectEventType(object):
    """
    Type of the distributed object event.
    """

    CREATED = "CREATED"
    """
    DistributedObject is created.
    """

    DESTROYED = "DESTROYED"
    """
    DistributedObject is destroyed.
    """


class DistributedObjectEvent(object):
    """
    Distributed Object Event
    """

    def __init__(self, name, service_name, event_type, source):
        self.name = name
        self.service_name = service_name
        self.event_type = DistributedObjectEventType.reverse.get(event_type, event_type)
        self.source = source

    def __repr__(self):
        return "DistributedObjectEvent(name=%s, service_name=%s, event_type=%s, source=%s)" \
               % (self.name, self.service_name, self.event_type, self.source)


class SimpleEntryView(object):
    """
    EntryView represents a readonly view of a map entry.
    """
    def __init__(self, key, value, cost, creation_time, expiration_time, hits, last_access_time,
                 last_stored_time, last_update_time, version, ttl, max_idle):
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
        The cost in bytes of the entry.
        """

        self.creation_time = creation_time
        """
        The creation time of the entry.
        """

        self.expiration_time = expiration_time
        """
        The expiration time of the entry.
        """

        self.hits = hits
        """
        Number of hits of the entry.
        """

        self.last_access_time = last_access_time
        """
        The last access time for the entry.
        """

        self.last_stored_time = last_stored_time
        """
        The last store time for the value.
        """

        self.last_update_time = last_update_time
        """
        The last time the value was updated.
        """

        self.version = version
        """
        The version of the entry.
        """

        self.ttl = ttl
        """
        The last set time to live milliseconds.
        """

        self.max_idle = max_idle
        """
        The last set max idle time in milliseconds.
        """

    def __repr__(self):
        return "SimpleEntryView(key=%s, value=%s, cost=%s, creation_time=%s, " \
               "expiration_time=%s, hits=%s, last_access_time=%s, last_stored_time=%s, " \
               "last_update_time=%s, version=%s, ttl=%s, max_idle=%s" \
               % (self.key, self.value, self.cost, self.creation_time, self.expiration_time, self.hits,
                  self.last_access_time, self.last_stored_time, self.last_update_time, self.version,
                  self.ttl, self.max_idle)


class MemberSelector(object):
    """
    Subclasses of this class select members
    that are capable of executing a special kind of task.
    The select(Member) method is called for every available
    member in the cluster and it is up to the implementation to decide
    if the member is going to be used or not.
    """

    def select(self, member):
        """
        Decides if the given member will be part of an operation or not.

        :param member: (:class:`~hazelcast.core.Member`), the member instance to decide upon.
        :return: (bool), True if the member should take part in the operation, False otherwise.
        """
        raise NotImplementedError()


class DataMemberSelector(MemberSelector):
    def select(self, member):
        return not member.is_lite_member


class HazelcastJsonValue(object):
    """
    HazelcastJsonValue is a wrapper for JSON formatted strings. It is preferred
    to store HazelcastJsonValue instead of Strings for JSON formatted strings.
    Users can run predicates and use indexes on the attributes of the underlying
    JSON strings.

    HazelcastJsonValue is queried using Hazelcast's querying language.
    See `Distributed Query section <https://github.com/hazelcast/hazelcast-python-client#77-distributed-query>`_.

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
        util.check_not_none(value, "JSON string or the object cannot be None.")
        if isinstance(value, six.string_types):
            self._json_string = value
        else:
            self._json_string = json.dumps(value)

    def to_string(self):
        """
        Returns unaltered string that was used to create this object.

        :return: (str), original string
        """
        return self._json_string

    def loads(self):
        """
        Deserializes the string that was used to create this object
        and returns as Python object.

        :return: (object), Python object represented by the original string
        """
        return json.loads(self._json_string)

    def __eq__(self, other):
        return isinstance(other, HazelcastJsonValue) and self._json_string == other._json_string

    def __ne__(self, other):
        return not self.__eq__(other)


class MemberVersion(object):
    __slots__ = ("major", "minor", "patch")

    def __init__(self, major, minor, patch):
        self.major = major
        self.minor = minor
        self.patch = patch
