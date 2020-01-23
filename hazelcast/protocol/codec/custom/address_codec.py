from hazelcast.protocol.client_message import ClientMessage, NULL_FRAME, BEGIN_FRAME, END_FRAME, PARTITION_ID_FIELD_OFFSET, RESPONSE_BACKUP_ACKS_FIELD_OFFSET, UNFRAGMENTED_MESSAGE, TYPE_FIELD_OFFSET
import hazelcast.protocol.bits as Bits
from hazelcast.protocol.codec.builtin import *
from hazelcast.protocol.codec.custom import *
from hazelcast.core import Address

# Generated("3379cf6bb330d268ec62cd7de98c146a")

PORT_FIELD_OFFSET = 0
INITIAL_FRAME_SIZE = PORT_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES

class AddressCodec:
    @staticmethod
    def encode(client_message, address):
        client_message.add(BEGIN_FRAME)

        initial_frame = ClientMessage.Frame(bytearray(INITIAL_FRAME_SIZE))
        FixedSizeTypesCodec.encode_int(initial_frame.content, PORT_FIELD_OFFSET, address.port)
        client_message.add(initial_frame)

        StringCodec.encode(client_message, address.host)

        client_message.add(END_FRAME)

    @staticmethod
    def decode(iterator):
        # begin frame
        iterator.next()

        initial_frame = iterator.next()
        port = FixedSizeTypesCodec.decode_int(initial_frame.content, PORT_FIELD_OFFSET)

        host = StringCodec.decode(iterator)

        CodecUtil.fast_forward_to_end_frame(iterator)

        return Address(host, port)