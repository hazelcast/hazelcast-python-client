"""Hazelcast Core objects"""

SERIALIZATION_VERSION = 1
CLIENT_TYPE = "PYH"


class Member(object):
    def __init__(self, address, uuid, is_lite_member, attributes):
        self.address = address
        self.uuid = uuid
        self.is_lite_member = is_lite_member
        self.attributes = attributes

    def __str__(self):
        return "Member [{}]:{}".format(self.address.host, self.address.port)

    def __repr__(self):
        return "Member(host={}, port={}, uuid={}, liteMember={}, attributes={})" \
            .format(self.address.host, self.address.port, self.uuid, self.is_lite_member, self.attributes)


class Address(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def __str__(self):
        return "Address(host=%s, port=%d)" % (self.host, self.port)

    def __repr__(self):
        return str(self)

    def __hash__(self):
        return hash((self.host, self.port))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and (self.host, self.port) == (other.host, other.port)


class DistributedObjectInfo(object):
    def __init__(self, name, service_name):
        self.name = name
        self.service_name = service_name

    def __str__(self):
        return "DistributedObjectInfo(name={}, serviceName={})".format(self.name, self.service_name)

    def __repr__(self):
        return str(self)


class EntryView(object):
    def __init__(self, key, value):
        self.key = key
        self.value = value
        self.cost = None
        self.creationTime = None
        self.expirationTime = None
        self.hits = None
        self.lastAccessTime = None
        self.lastStoredTime = None
        self.lastUpdateTime = None
        self.version = None
        self.evictionCriteriaNumber = None
        self.ttl = None
