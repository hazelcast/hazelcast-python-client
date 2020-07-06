"""
Hazelcast client protocol codecs
"""

from collections import namedtuple
# TODO: Potential cyclic dependent import
# from hazelcast.protocol.codec.builtin import errors_codec

EXCEPTION_MESSAGE_TYPE = 0


StackTraceElement = namedtuple('StackTraceElement', ['declaring_class', 'method_name', 'file_name', 'line_number'])


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


