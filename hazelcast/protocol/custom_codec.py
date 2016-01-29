"""
Hazelcast client protocol codecs
"""

from collections import namedtuple
from hazelcast.core import Member, DistributedObjectInfo, EntryView, Address

EXCEPTION_MESSAGE_TYPE = 109


class MemberCodec(object):
    @classmethod
    def encode(cls, client_message, member):
        AddressCodec.encode(client_message, member.address)
        client_message.append_str(member.uuid)
        client_message.append_bool(member.is_lite_member)
        client_message.append_int(len(member.attributes))
        for key, value in member.attributes:
            client_message.append_str(key)
            client_message.append_str(value)

    @classmethod
    def decode(cls, client_message, to_object=None):
        address = AddressCodec.decode(client_message)
        uuid = client_message.read_str()
        lite_member = client_message.read_bool()
        attribute_size = client_message.read_int()
        attributes = {}
        for i in range(0, attribute_size, 1):
            key = client_message.read_str()
            value = client_message.read_str()
            attributes[key] = value
        return Member(address, uuid, attributes, lite_member)


class AddressCodec(object):
    @classmethod
    def encode(cls, client_message, obj):
        client_message.append_str(obj.host).append_int(obj.port)

    @classmethod
    def decode(cls, client_message, to_object=None):
        host = client_message.read_str()
        port = client_message.read_int()
        return Address(host, port)


class DistributedObjectInfoCodec(object):
    @classmethod
    def encode(cls, client_message, obj):
        client_message.append_str(obj.service_name).append_str(obj.name)

    @classmethod
    def decode(cls, client_message):
        service_name = client_message.read_str()
        name = client_message.read_str()
        return DistributedObjectInfo(name, service_name)


class EntryViewCodec(object):
    @classmethod
    def encode(cls, client_message, entry_view):
        client_message.append_data(entry_view.key)
        client_message.append_data(entry_view.value)
        client_message.append_long(entry_view.cost)
        client_message.append_long(entry_view.cost)
        client_message.append_long(entry_view.creationTime)
        client_message.append_long(entry_view.expirationTime)
        client_message.append_long(entry_view.hits)
        client_message.append_long(entry_view.lastAccessTime)
        client_message.append_long(entry_view.lastStoredTime)
        client_message.append_long(entry_view.lastUpdateTime)
        client_message.append_long(entry_view.version)
        client_message.append_long(entry_view.evictionCriteriaNumber)
        client_message.append_long(entry_view.ttl)

    @classmethod
    def decode(cls, client_message, to_object):
        entry_view = EntryView()
        entry_view.key = to_object(client_message.read_data())
        entry_view.value = to_object(client_message.read_data())
        entry_view.cost = client_message.read_long()
        entry_view.creation_time = client_message.read_long()
        entry_view.expiration_time = client_message.read_long()
        entry_view.hits = client_message.read_long()
        entry_view.last_access_time = client_message.read_long()
        entry_view.last_stored_time = client_message.read_long()
        entry_view.last_update_time = client_message.read_long()
        entry_view.version = client_message.read_long()
        entry_view.eviction_criteria_number = client_message.read_long()
        entry_view.ttl = client_message.read_long()
        return entry_view


class QueryCacheEventDataCodec(object):
    @classmethod
    def encode(cls, client_message, obj):
        pass

    @classmethod
    def decode(cls, client_message, to_object=None):
        pass


StackTraceElement = namedtuple('StackTraceElement', ['declaring_class', 'method_name', 'file_name', 'line_number'])


class ErrorCodec(object):
    message = None
    cause_class_name = None

    def __init__(self, client_message):
        self.error_code = client_message.read_int()
        self.class_name = client_message.read_str()
        if not client_message.read_bool():
            self.message = client_message.read_str()

        self.stack_trace = []
        stack_trace_count = client_message.read_int()
        for _ in xrange(stack_trace_count):
            self.stack_trace.append(self.decode_stack_trace(client_message))

        self.cause_error_code = client_message.read_int()
        if not client_message.read_bool():
            self.cause_class_name = client_message.read_str()

    @staticmethod
    def decode_stack_trace(client_message):
        declaring_class = client_message.read_str()
        method_name = client_message.read_str()
        file_name = None
        if not client_message.read_bool():
            file_name = client_message.read_str()
        line_number = client_message.read_int()
        return StackTraceElement(declaring_class=declaring_class,
                                 method_name=method_name, file_name=file_name, line_number=line_number)

    def __repr__(self):
        return 'ErrorCodec(error_code="%s", class_name="%s", message="%s", cause_error_code="%s", ' \
               'cause_class_name="%s' % (self.error_code, self.class_name, self.message, self.cause_error_code,
                                         self.cause_class_name)
