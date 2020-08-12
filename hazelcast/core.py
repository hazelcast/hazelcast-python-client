"""Hazelcast Core objects"""
import json

from hazelcast import six
from hazelcast import util
from hazelcast.util import enum


class MemberInfo(object):
    __slots__ = ("address", "uuid", "attributes", "lite_member", "version")

    """
    Represents a member in the cluster with its address, uuid, lite member status, attributes and version.
    """
    def __init__(self, address, uuid, attributes, lite_member, version, *args):
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
    def __init__(self, name, service_name):
        self.name = name
        self.service_name = service_name

    def __repr__(self):
        return "DistributedObjectInfo(name={}, serviceName={})".format(self.name, self.service_name)

    def __hash__(self):
        return hash((self.name, self.service_name))

    def __eq__(self, other):
        if isinstance(other, DistributedObjectInfo):
            return self.name == other.name and self.service_name == other.service_name
        return False


DistributedObjectEventType = enum(CREATED="CREATED", DESTROYED="DESTROYED")
"""
Type of the distributed object event.

* CREATED : DistributedObject is created.
* DESTROYED: DistributedObject is destroyed. 
"""


class DistributedObjectEvent(object):
    """
    Distributed Object Event
    """

    def __init__(self, name, service_name, event_type):
        self.name = name
        self.service_name = service_name
        self.event_type = DistributedObjectEventType.reverse.get(event_type, None)

    def __repr__(self):
        return "DistributedObjectEvent[name={}, " \
               "service_name={}, " \
               "event_type={}]".format(self.name, self.service_name, self.event_type)


class EntryView(object):
    """
    EntryView represents a readonly view of a map entry.
    """
    key = None
    """
    The key of the entry.
    """
    value = None
    """
    The value of the entry.
    """
    cost = None
    """
    The cost in bytes of the entry.
    """
    creation_time = None
    """
    The creation time of the entry.
    """
    expiration_time = None
    """
    The expiration time of the entry.
    """
    hits = None
    """
    Number of hits of the entry.
    """
    last_access_time = None
    """
    The last access time for the entry.
    """
    last_stored_time = None
    """
    The last store time for the value.
    """
    last_update_time = None
    """
    The last time the value was updated.
    """
    version = None
    """
    The version of the entry.
    """
    eviction_criteria_number = None
    """
    The criteria number for eviction.
    """
    ttl = None
    """
    The last set time to live second.
    """

    def __repr__(self):
        return "EntryView(key=%s, value=%s, cost=%s, creation_time=%s, expiration_time=%s, hits=%s, last_access_time=%s, " \
               "last_stored_time=%s, last_update_time=%s, version=%s, eviction_criteria_number=%s, ttl=%s" % (
                   self.key, self.value, self.cost, self.creation_time, self.expiration_time, self.hits,
                   self.last_access_time, self.last_stored_time, self.last_update_time, self.version,
                   self.eviction_criteria_number, self.ttl)


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
