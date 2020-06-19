from hazelcast.protocol.client_message import ClientMessage, NULL_FRAME, BEGIN_FRAME, END_FRAME, PARTITION_ID_FIELD_OFFSET, RESPONSE_BACKUP_ACKS_FIELD_OFFSET, UNFRAGMENTED_MESSAGE, TYPE_FIELD_OFFSET
import hazelcast.protocol.bits as Bits
from hazelcast.protocol.codec.builtin import *
from hazelcast.protocol.codec.custom import *
from hazelcast.core import Address

# Generated("6cea8795fb28f977aeb22fbca3290c3b")

PORT_FIELD_OFFSET = 0
INITIAL_FRAME_SIZE = PORT_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES


def encode(client_message, address):
    client_message.add(BEGIN_FRAME)

    initial_frame = ClientMessage.Frame(bytearray(INITIAL_FRAME_SIZE))
    fixed_size_types_codec.encode_int(initial_frame.content, PORT_FIELD_OFFSET, address.port)
    client_message.add(initial_frame)

    string_codec.encode(client_message, address.host)

    client_message.add(END_FRAME)


def decode(iterator):
    # begin frame
    iterator.next()

    initial_frame = iterator.next()
    port = fixed_size_types_codec.decode_int(initial_frame.content, PORT_FIELD_OFFSET)

    host = string_codec.decode(iterator)

    codec_util.fast_forward_to_end_frame(iterator)

    return Address(host, port)