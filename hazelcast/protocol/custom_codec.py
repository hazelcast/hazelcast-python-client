"""
Hazelcast client protocol codecs
"""

from collections import namedtuple
from hazelcast.core import Member, DistributedObjectInfo, EntryView, Address
# TODO: Potential cyclic dependent import
# from hazelcast.protocol.codec.builtin import ErrorsCodec
from hazelcast.six.moves import range

EXCEPTION_MESSAGE_TYPE = 0


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
        return Member(address, uuid, lite_member, attributes)


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


class ExceptionFactory(object):
    _code_to_factory = {}

    def __init__(self):
        self.register()

    def register(self, code, exception_factory):
        self._code_to_factory[code] = exception_factory


class ErrorCodec(object):
    message = None
    cause_class_name = None

    # TODO: Implement the stack trace elements field and handle all error holders.
    def __init__(self, client_message):
        error_holders = ErrorsCodec.decode(client_message)
        self.error_code = error_holders[0].error_code
        self.class_name = error_holders[0].class_name
        self.message = error_holders[0].message

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
        return 'ErrorCodec(error_code="%s", class_name="%s", message="%s"'\
               % (self.error_code, self.class_name, self.message)



from hazelcast.protocol.client_message import ClientMessage,RESPONSE_BACKUP_ACKS_FIELD_OFFSET,UNFRAGMENTED_MESSAGE
from hazelcast.protocol.bits import BYTE_SIZE_IN_BYTES
from hazelcast.protocol.codec.builtin.list_multi_frame_codec import ListMultiFrameCodec
from hazelcast.protocol.codec.custom.error_holder_codec import ErrorHolderCodec

INITIAL_FRAME_SIZE = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES

class ErrorsCodec:
    @staticmethod
    def encode(error_holders):
        client_message = ClientMessage.create_for_encode()
        initial_frame = ClientMessage.Frame(bytearray(INITIAL_FRAME_SIZE), UNFRAGMENTED_MESSAGE)
        client_message.add(initial_frame)
        client_message.set_message_type(EXCEPTION_MESSAGE_TYPE)
        ListMultiFrameCodec.encode(client_message, error_holders, ErrorHolderCodec.encode)
        return client_message

    @staticmethod
    def decode(client_message):
        iterator = client_message.frame_iterator()

        iterator.next()
        return ListMultiFrameCodec.decode(iterator, ErrorHolderCodec.decode)
