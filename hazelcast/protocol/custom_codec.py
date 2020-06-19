"""
Hazelcast client protocol codecs
"""

from collections import namedtuple
from hazelcast.core import MemberInfo, DistributedObjectInfo, Address
# TODO: Potential cyclic dependent import
# from hazelcast.protocol.codec.builtin import ErrorsCodec
from hazelcast.six.moves import range

EXCEPTION_MESSAGE_TYPE = 0


class MemberCodec(object):
    @classmethod
    def encode(cls, client_message, member):
        AddressCodec.encode(client_message, member.address)
        client_message.append_str(member.uuid)
        client_message.append_bool(member.lite_member)
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
        return MemberInfo(address, uuid, lite_member, attributes)


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
from hazelcast.protocol.codec.builtin import list_multi_frame_codec
from hazelcast.protocol.codec.custom import error_holder_codec

INITIAL_FRAME_SIZE = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES

class ErrorsCodec:
    @staticmethod
    def encode(error_holders):
        client_message = ClientMessage.create_for_encode()
        initial_frame = ClientMessage.Frame(bytearray(INITIAL_FRAME_SIZE), UNFRAGMENTED_MESSAGE)
        client_message.add(initial_frame)
        client_message.set_message_type(EXCEPTION_MESSAGE_TYPE)
        list_multi_frame_codec.encode(client_message, error_holders, error_holder_codec.encode)
        return client_message

    @staticmethod
    def decode(client_message):
        iterator = client_message.frame_iterator()

        iterator.next()
        return list_multi_frame_codec.decode(iterator, error_holder_codec.decode)
