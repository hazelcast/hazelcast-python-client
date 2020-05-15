from hazelcast.protocol.client_message import ClientMessage, NULL_FRAME, BEGIN_FRAME, END_FRAME, PARTITION_ID_FIELD_OFFSET, RESPONSE_BACKUP_ACKS_FIELD_OFFSET, UNFRAGMENTED_MESSAGE, TYPE_FIELD_OFFSET
import hazelcast.protocol.bits as Bits
from hazelcast.protocol.codec.builtin import *
from hazelcast.protocol.codec.custom import *
from hazelcast.config import BitmapIndexOptions

# Generated("56fd1ee5dca89e0000cb6216e3a4588f")

UNIQUE_KEY_TRANSFORMATION_FIELD_OFFSET = 0
INITIAL_FRAME_SIZE = UNIQUE_KEY_TRANSFORMATION_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES


class BitmapIndexOptionsCodec(object):
    @staticmethod
    def encode(client_message, bitmap_index_options):
        client_message.add(BEGIN_FRAME)

        initial_frame = ClientMessage.Frame(bytearray(INITIAL_FRAME_SIZE))
        FixedSizeTypesCodec.encode_int(initial_frame.content, UNIQUE_KEY_TRANSFORMATION_FIELD_OFFSET, bitmap_index_options.unique_key_transformation)
        client_message.add(initial_frame)

        StringCodec.encode(client_message, bitmap_index_options.unique_key)

        client_message.add(END_FRAME)

    @staticmethod
    def decode(iterator):
        # begin frame
        iterator.next()

        initial_frame = iterator.next()
        unique_key_transformation = FixedSizeTypesCodec.decode_int(initial_frame.content, UNIQUE_KEY_TRANSFORMATION_FIELD_OFFSET)

        unique_key = StringCodec.decode(iterator)

        CodecUtil.fast_forward_to_end_frame(iterator)

        return BitmapIndexOptions(unique_key, unique_key_transformation)