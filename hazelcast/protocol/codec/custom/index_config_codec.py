from hazelcast.protocol.client_message import ClientMessage, NULL_FRAME, BEGIN_FRAME, END_FRAME, PARTITION_ID_FIELD_OFFSET, RESPONSE_BACKUP_ACKS_FIELD_OFFSET, UNFRAGMENTED_MESSAGE, TYPE_FIELD_OFFSET
import hazelcast.protocol.bits as Bits
from hazelcast.protocol.codec.builtin import *
from hazelcast.protocol.codec.custom import *
from hazelcast.config import IndexConfig

# Generated("6735605f2429ef2b7c8fb14748bd0c1f")

TYPE_FIELD_OFFSET = 0
INITIAL_FRAME_SIZE = TYPE_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES


class IndexConfigCodec(object):
    @staticmethod
    def encode(client_message, index_config):
        client_message.add(BEGIN_FRAME)

        initial_frame = ClientMessage.Frame(bytearray(INITIAL_FRAME_SIZE))
        FixedSizeTypesCodec.encode_int(initial_frame.content, TYPE_FIELD_OFFSET, index_config.type)
        client_message.add(initial_frame)

        CodecUtil.encode_nullable(client_message, index_config.name, StringCodec.encode)
        ListMultiFrameCodec.encode(client_message, index_config.attributes, StringCodec.encode)
        CodecUtil.encode_nullable(client_message, index_config.bitmap_index_options, BitmapIndexOptionsCodec.encode)

        client_message.add(END_FRAME)

    @staticmethod
    def decode(iterator):
        # begin frame
        iterator.next()

        initial_frame = iterator.next()
        type = FixedSizeTypesCodec.decode_int(initial_frame.content, TYPE_FIELD_OFFSET)

        name = CodecUtil.decode_nullable(iterator, StringCodec.decode)
        attributes = ListMultiFrameCodec.decode(iterator, StringCodec.decode)
        bitmap_index_options = CodecUtil.decode_nullable(iterator, BitmapIndexOptionsCodec.decode)

        CodecUtil.fast_forward_to_end_frame(iterator)

        return IndexConfig(name, type, attributes, bitmap_index_options)