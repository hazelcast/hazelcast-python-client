from hazelcast.protocol.builtin import FixSizedTypesCodec, CodecUtil
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import END_FRAME_BUF, END_FINAL_FRAME_BUF, SIZE_OF_FRAME_LENGTH_AND_FLAGS, create_initial_buffer_custom
from hazelcast.vector import VectorSearchOptions
from hazelcast.protocol.builtin import MapCodec
from hazelcast.protocol.builtin import StringCodec

_INCLUDE_VALUE_ENCODE_OFFSET = 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS
_INCLUDE_VALUE_DECODE_OFFSET = 0
_INCLUDE_VECTORS_ENCODE_OFFSET = _INCLUDE_VALUE_ENCODE_OFFSET + BOOLEAN_SIZE_IN_BYTES
_INCLUDE_VECTORS_DECODE_OFFSET = _INCLUDE_VALUE_DECODE_OFFSET + BOOLEAN_SIZE_IN_BYTES
_LIMIT_ENCODE_OFFSET = _INCLUDE_VECTORS_ENCODE_OFFSET + BOOLEAN_SIZE_IN_BYTES
_LIMIT_DECODE_OFFSET = _INCLUDE_VECTORS_DECODE_OFFSET + BOOLEAN_SIZE_IN_BYTES
_INITIAL_FRAME_SIZE = _LIMIT_ENCODE_OFFSET + INT_SIZE_IN_BYTES - SIZE_OF_FRAME_LENGTH_AND_FLAGS


class VectorSearchOptionsCodec:
    @staticmethod
    def encode(buf, vector_search_options, is_final=False):
        initial_frame_buf = create_initial_buffer_custom(_INITIAL_FRAME_SIZE)
        FixSizedTypesCodec.encode_boolean(initial_frame_buf, _INCLUDE_VALUE_ENCODE_OFFSET, vector_search_options.include_value)
        FixSizedTypesCodec.encode_boolean(initial_frame_buf, _INCLUDE_VECTORS_ENCODE_OFFSET, vector_search_options.include_vectors)
        FixSizedTypesCodec.encode_int(initial_frame_buf, _LIMIT_ENCODE_OFFSET, vector_search_options.limit)
        buf.extend(initial_frame_buf)
        MapCodec.encode_nullable(buf, vector_search_options.hints, StringCodec.encode, StringCodec.encode)
        if is_final:
            buf.extend(END_FINAL_FRAME_BUF)
        else:
            buf.extend(END_FRAME_BUF)

    @staticmethod
    def decode(msg):
        msg.next_frame()
        initial_frame = msg.next_frame()
        include_value = FixSizedTypesCodec.decode_boolean(initial_frame.buf, _INCLUDE_VALUE_DECODE_OFFSET)
        include_vectors = FixSizedTypesCodec.decode_boolean(initial_frame.buf, _INCLUDE_VECTORS_DECODE_OFFSET)
        limit = FixSizedTypesCodec.decode_int(initial_frame.buf, _LIMIT_DECODE_OFFSET)
        hints = MapCodec.decode_nullable(msg, StringCodec.decode, StringCodec.decode)
        CodecUtil.fast_forward_to_end_frame(msg)
        return VectorSearchOptions(include_value, include_vectors, limit, hints)
