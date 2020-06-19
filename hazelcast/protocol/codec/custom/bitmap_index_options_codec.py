from hazelcast.protocol.client_message import ClientMessage, NULL_FRAME, BEGIN_FRAME, END_FRAME, PARTITION_ID_FIELD_OFFSET, RESPONSE_BACKUP_ACKS_FIELD_OFFSET, UNFRAGMENTED_MESSAGE, TYPE_FIELD_OFFSET
import hazelcast.protocol.bits as Bits
from hazelcast.protocol.codec.builtin import *
from hazelcast.protocol.codec.custom import *
from hazelcast.config import BitmapIndexOptions

# Generated("ffa78510fd1a5e744fb17930a306bd53")

UNIQUE_KEY_TRANSFORMATION_FIELD_OFFSET = 0
INITIAL_FRAME_SIZE = UNIQUE_KEY_TRANSFORMATION_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES


def encode(client_message, bitmap_index_options):
    client_message.add(BEGIN_FRAME)

    initial_frame = ClientMessage.Frame(bytearray(INITIAL_FRAME_SIZE))
    fixed_size_types_codec.encode_int(initial_frame.content, UNIQUE_KEY_TRANSFORMATION_FIELD_OFFSET, bitmap_index_options.unique_key_transformation)
    client_message.add(initial_frame)

    string_codec.encode(client_message, bitmap_index_options.unique_key)

    client_message.add(END_FRAME)


def decode(iterator):
    # begin frame
    iterator.next()

    initial_frame = iterator.next()
    unique_key_transformation = fixed_size_types_codec.decode_int(initial_frame.content, UNIQUE_KEY_TRANSFORMATION_FIELD_OFFSET)

    unique_key = string_codec.decode(iterator)

    codec_util.fast_forward_to_end_frame(iterator)

    return BitmapIndexOptions(unique_key, unique_key_transformation)