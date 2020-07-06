from hazelcast.protocol.client_message import ClientMessage,RESPONSE_BACKUP_ACKS_FIELD_OFFSET,UNFRAGMENTED_MESSAGE
from hazelcast.protocol.bits import BYTE_SIZE_IN_BYTES
from hazelcast.protocol.codec.builtin import list_multi_frame_codec
# from hazelcast.protocol.codec.custom import error_holder_codec


EXCEPTION_MESSAGE_TYPE = 0
INITIAL_FRAME_SIZE = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES


def encode(error_holders):
    client_message = ClientMessage.create_for_encode()
    initial_frame = ClientMessage.Frame(bytearray(INITIAL_FRAME_SIZE), UNFRAGMENTED_MESSAGE)
    client_message.add(initial_frame)
    client_message.set_message_type(EXCEPTION_MESSAGE_TYPE)
    list_multi_frame_codec.encode(client_message, error_holders, error_holder_codec.encode)
    return client_message


def decode(client_message):
    iterator = client_message.frame_iterator()

    iterator.next()
    return list_multi_frame_codec.decode(iterator, error_holder_codec.decode)

