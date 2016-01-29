"""Hazelcast Core objects"""

SERIALIZATION_VERSION = 1
CLIENT_TYPE = "PYH"


class Member(object):
    def __init__(self, address, uuid, is_lite_member=False, attributes={}):
        self.address = address
        self.uuid = uuid
        self.is_lite_member = is_lite_member
        self.attributes = attributes

    def __str__(self):
        return "Member [{}]:{}".format(self.address.host, self.address.port)

    def __repr__(self):
        return "Member(host={}, port={}, uuid={}, liteMember={}, attributes={})" \
            .format(self.address.host, self.address.port, self.uuid, self.is_lite_member, self.attributes)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.address == other.address


class Address(object):
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
    def __init__(self, name, service_name):
        self.name = name
        self.service_name = service_name

    def __repr__(self):
        return "DistributedObjectInfo(name={}, serviceName={})".format(self.name, self.service_name)


class EntryView(object):
    key = None
    value = None
    cost = None
    creation_time = None
    expiration_time = None
    hits = None
    last_access_time = None
    last_stored_time = None
    last_update_time = None
    version = None
    eviction_criteria_number = None
    ttl = None

    def __repr__(self):
        return "EntryView(key=%s, value=%s, cost=%s, creation_time=%s, expiration_time=%s, hits=%s, last_access_time=%s, " \
               "last_stored_time=%s, last_update_time=%s, version=%s, eviction_criteria_number=%s, ttl=%s" % (
                   self.key, self.value, self.cost, self.creation_time, self.expiration_time, self.hits,
                   self.last_access_time, self.last_stored_time, self.last_update_time, self.version,
                   self.eviction_criteria_number, self.ttl)
