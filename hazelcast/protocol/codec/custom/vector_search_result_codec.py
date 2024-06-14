from hazelcast.protocol.builtin import FixSizedTypesCodec, CodecUtil
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import END_FRAME_BUF, END_FINAL_FRAME_BUF, SIZE_OF_FRAME_LENGTH_AND_FLAGS, create_initial_buffer_custom
from hazelcast.vector import VectorSearchResult
from hazelcast.protocol.builtin import DataCodec
from hazelcast.protocol.builtin import ListMultiFrameCodec
from hazelcast.protocol.codec.custom.vector_pair_codec import VectorPairCodec

_SCORE_ENCODE_OFFSET = 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS
_SCORE_DECODE_OFFSET = 0
_INITIAL_FRAME_SIZE = _SCORE_ENCODE_OFFSET + FLOAT_SIZE_IN_BYTES - SIZE_OF_FRAME_LENGTH_AND_FLAGS


class VectorSearchResultCodec:
    @staticmethod
    def encode(buf, vector_search_result, is_final=False):
        initial_frame_buf = create_initial_buffer_custom(_INITIAL_FRAME_SIZE)
        FixSizedTypesCodec.encode_float(initial_frame_buf, _SCORE_ENCODE_OFFSET, vector_search_result.score)
        buf.extend(initial_frame_buf)
        DataCodec.encode(buf, vector_search_result.key)
        CodecUtil.encode_nullable(buf, vector_search_result.value, DataCodec.encode)
        ListMultiFrameCodec.encode_nullable(buf, vector_search_result.vectors, VectorPairCodec.encode)
        if is_final:
            buf.extend(END_FINAL_FRAME_BUF)
        else:
            buf.extend(END_FRAME_BUF)

    @staticmethod
    def decode(msg):
        msg.next_frame()
        initial_frame = msg.next_frame()
        score = FixSizedTypesCodec.decode_float(initial_frame.buf, _SCORE_DECODE_OFFSET)
        key = DataCodec.decode(msg)
        value = CodecUtil.decode_nullable(msg, DataCodec.decode)
        vectors = ListMultiFrameCodec.decode_nullable(msg, VectorPairCodec.decode)
        CodecUtil.fast_forward_to_end_frame(msg)
        return VectorSearchResult(key, value, score, vectors)
