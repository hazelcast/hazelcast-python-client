from hazelcast.protocol.client_message import ClientMessage, NULL_FRAME, BEGIN_FRAME, END_FRAME, PARTITION_ID_FIELD_OFFSET, RESPONSE_BACKUP_ACKS_FIELD_OFFSET, UNFRAGMENTED_MESSAGE, TYPE_FIELD_OFFSET
import hazelcast.protocol.bits as Bits
from hazelcast.protocol.codec.builtin import *
from hazelcast.protocol.codec.custom import *
from hazelcast.config import IndexConfig

# Generated("f4ec29bc3b6874ba2d676eb9c9b6dd1b")

TYPE_FIELD_OFFSET = 0
INITIAL_FRAME_SIZE = TYPE_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES


def encode(client_message, index_config):
    client_message.add(BEGIN_FRAME)

    initial_frame = ClientMessage.Frame(bytearray(INITIAL_FRAME_SIZE))
    fixed_size_types_codec.encode_int(initial_frame.content, TYPE_FIELD_OFFSET, index_config.type)
    client_message.add(initial_frame)

    codec_util.encode_nullable(client_message, index_config.name, string_codec.encode)
    list_multi_frame_codec.encode(client_message, index_config.attributes, string_codec.encode)
    codec_util.encode_nullable(client_message, index_config.bitmap_index_options, bitmap_index_options_codec.encode)

    client_message.add(END_FRAME)


def decode(iterator):
    # begin frame
    iterator.next()

    initial_frame = iterator.next()
    type = fixed_size_types_codec.decode_int(initial_frame.content, TYPE_FIELD_OFFSET)

    name = codec_util.decode_nullable(iterator, string_codec.decode)
    attributes = list_multi_frame_codec.decode(iterator, string_codec.decode)
    bitmap_index_options = codec_util.decode_nullable(iterator, bitmap_index_options_codec.decode)

    codec_util.fast_forward_to_end_frame(iterator)

    return IndexConfig(name, type, attributes, bitmap_index_options)