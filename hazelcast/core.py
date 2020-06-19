"""Hazelcast Core objects"""
import json

from hazelcast import six
from hazelcast import util
from hazelcast.util import enum
from hazelcast.serialization.api import IdentifiedDataSerializable


class MemberInfo(object):
    def __init__(self, address, uuid, attributes={}, is_lite_member=False, version=None):
        self.address = address
        self.uuid = uuid
        self.lite_member = is_lite_member
        self.attributes = attributes
        self.version = version

    def __str__(self):
        return "MemberInfo [{}]:{} - {}".format(self.address.host, self.address.port, self.uuid)

    def __repr__(self):
        return "MemberInfo(host={}, port={}, uuid={}, liteMember={}, attributes={})" \
            .format(self.address.host, self.address.port, self.uuid, self.lite_member, self.attributes)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.address == other.address and self.uuid == other.uuid


class MemberVersion(object):
    def __init__(self, major, minor, patch):
        self.major = major
        self.minor = minor
        self.patch = patch

    def get_major(self):
        return self.major

    def get_minor(self):
        return self.minor

    def get_patch(self):
        return self.patch


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


class SimpleEntryView(object):
    def __init__(self, key, value, cost, creation_time, expiration_time, hits, last_access_time, last_stored_time,
                 last_update_time, version, ttl, max_idle):
        self.key = key
        self.value = value
        self.cost = cost
        self.creation_time = creation_time
        self.expiration_time = expiration_time
        self.hits = hits
        self.last_access_time = last_access_time
        self.last_stored_time = last_stored_time
        self.last_update_time = last_update_time
        self.version = version
        self.ttl = ttl
        self.max_idle = max_idle

    def __repr__(self):
        return "SimpleEntryView(key=%s, value=%s, cost=%s, creation_time=%s, expiration_time=%s, hits=%s, " \
               "last_access_time=%s, last_stored_time=%s, last_update_time=%s, version=%s, ttl=%s, max_idle=%s" % (
                   self.key, self.value, self.cost, self.creation_time, self.expiration_time, self.hits,
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
        return not member.lite_member


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
