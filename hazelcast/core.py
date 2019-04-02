"""Hazelcast Core objects"""
import json

from hazelcast import six
from hazelcast import util


class Member(object):
    """
    Represents a member in the cluster with its address, uuid, lite member status and attributes.
    """
    def __init__(self, address, uuid, is_lite_member=False, attributes={}):
        self.address = address
        self.uuid = uuid
        self.is_lite_member = is_lite_member
        self.attributes = attributes

    def __str__(self):
        return "Member [{}]:{} - {}".format(self.address.host, self.address.port, self.uuid)

    def __repr__(self):
        return "Member(host={}, port={}, uuid={}, liteMember={}, attributes={})" \
            .format(self.address.host, self.address.port, self.uuid, self.is_lite_member, self.attributes)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.address == other.address and self.uuid == other.uuid


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
        return isinstance(other, self.__class__) and (self.host, self.port) == (other.host, other.port)


class DistributedObjectInfo(object):
    """
    Represents name of the Distributed Object and the name of service which it belongs to.
    """
    def __init__(self, name, service_name):
        self.name = name
        self.service_name = service_name

    def __repr__(self):
        return "DistributedObjectInfo(name={}, serviceName={})".format(self.name, self.service_name)


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
