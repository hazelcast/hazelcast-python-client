from hazelcast.protocol.builtin import FixSizedTypesCodec, CodecUtil
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import END_FRAME_BUF, END_FINAL_FRAME_BUF, SIZE_OF_FRAME_LENGTH_AND_FLAGS, create_initial_buffer_custom
from hazelcast.vector import VectorPair
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import FloatArrayCodec

_TYPE_ENCODE_OFFSET = 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS
_TYPE_DECODE_OFFSET = 0
_INITIAL_FRAME_SIZE = _TYPE_ENCODE_OFFSET + BYTE_SIZE_IN_BYTES - SIZE_OF_FRAME_LENGTH_AND_FLAGS


class VectorPairCodec:
    @staticmethod
    def encode(buf, vector_pair, is_final=False):
        initial_frame_buf = create_initial_buffer_custom(_INITIAL_FRAME_SIZE)
        FixSizedTypesCodec.encode_byte(initial_frame_buf, _TYPE_ENCODE_OFFSET, vector_pair.type)
        buf.extend(initial_frame_buf)
        StringCodec.encode(buf, vector_pair.name)
        CodecUtil.encode_nullable(buf, vector_pair.vector, FloatArrayCodec.encode)
        if is_final:
            buf.extend(END_FINAL_FRAME_BUF)
        else:
            buf.extend(END_FRAME_BUF)

    @staticmethod
    def decode(msg):
        msg.next_frame()
        initial_frame = msg.next_frame()
        type = FixSizedTypesCodec.decode_byte(initial_frame.buf, _TYPE_DECODE_OFFSET)
        name = StringCodec.decode(msg)
        vector = CodecUtil.decode_nullable(msg, FloatArrayCodec.decode)
        CodecUtil.fast_forward_to_end_frame(msg)
        return VectorPair(name, type, vector)
