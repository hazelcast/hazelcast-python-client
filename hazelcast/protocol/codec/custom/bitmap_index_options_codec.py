from hazelcast.protocol.builtin import FixSizedTypesCodec, CodecUtil
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import END_FRAME_BUF, END_FINAL_FRAME_BUF, SIZE_OF_FRAME_LENGTH_AND_FLAGS, create_initial_buffer_custom
from hazelcast.config import BitmapIndexOptions
from hazelcast.protocol.builtin import StringCodec

_UNIQUE_KEY_TRANSFORMATION_ENCODE_OFFSET = 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS
_UNIQUE_KEY_TRANSFORMATION_DECODE_OFFSET = 0
_INITIAL_FRAME_SIZE = _UNIQUE_KEY_TRANSFORMATION_ENCODE_OFFSET + INT_SIZE_IN_BYTES - 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS


class BitmapIndexOptionsCodec(object):
    @staticmethod
    def encode(buf, bitmap_index_options, is_final=False):
        initial_frame_buf = create_initial_buffer_custom(_INITIAL_FRAME_SIZE)
        FixSizedTypesCodec.encode_int(initial_frame_buf, _UNIQUE_KEY_TRANSFORMATION_ENCODE_OFFSET, bitmap_index_options.unique_key_transformation)
        buf.extend(initial_frame_buf)
        StringCodec.encode(buf, bitmap_index_options.unique_key)
        if is_final:
            buf.extend(END_FINAL_FRAME_BUF)
        else:
            buf.extend(END_FRAME_BUF)

    @staticmethod
    def decode(msg):
        msg.next_frame()
        initial_frame = msg.next_frame()
        unique_key_transformation = FixSizedTypesCodec.decode_int(initial_frame.buf, _UNIQUE_KEY_TRANSFORMATION_DECODE_OFFSET)
        unique_key = StringCodec.decode(msg)
        CodecUtil.fast_forward_to_end_frame(msg)
        return BitmapIndexOptions(unique_key, unique_key_transformation)
