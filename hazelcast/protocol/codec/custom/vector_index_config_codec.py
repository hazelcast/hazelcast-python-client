from hazelcast.protocol.builtin import FixSizedTypesCodec, CodecUtil
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import END_FRAME_BUF, END_FINAL_FRAME_BUF, SIZE_OF_FRAME_LENGTH_AND_FLAGS, create_initial_buffer_custom
from hazelcast.vector import VectorIndexConfig
from hazelcast.protocol.builtin import StringCodec

_METRIC_ENCODE_OFFSET = 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS
_METRIC_DECODE_OFFSET = 0
_DIMENSION_ENCODE_OFFSET = _METRIC_ENCODE_OFFSET + INT_SIZE_IN_BYTES
_DIMENSION_DECODE_OFFSET = _METRIC_DECODE_OFFSET + INT_SIZE_IN_BYTES
_MAX_DEGREE_ENCODE_OFFSET = _DIMENSION_ENCODE_OFFSET + INT_SIZE_IN_BYTES
_MAX_DEGREE_DECODE_OFFSET = _DIMENSION_DECODE_OFFSET + INT_SIZE_IN_BYTES
_EF_CONSTRUCTION_ENCODE_OFFSET = _MAX_DEGREE_ENCODE_OFFSET + INT_SIZE_IN_BYTES
_EF_CONSTRUCTION_DECODE_OFFSET = _MAX_DEGREE_DECODE_OFFSET + INT_SIZE_IN_BYTES
_USE_DEDUPLICATION_ENCODE_OFFSET = _EF_CONSTRUCTION_ENCODE_OFFSET + INT_SIZE_IN_BYTES
_USE_DEDUPLICATION_DECODE_OFFSET = _EF_CONSTRUCTION_DECODE_OFFSET + INT_SIZE_IN_BYTES
_INITIAL_FRAME_SIZE = _USE_DEDUPLICATION_ENCODE_OFFSET + BOOLEAN_SIZE_IN_BYTES - SIZE_OF_FRAME_LENGTH_AND_FLAGS


class VectorIndexConfigCodec:
    @staticmethod
    def encode(buf, vector_index_config, is_final=False):
        initial_frame_buf = create_initial_buffer_custom(_INITIAL_FRAME_SIZE)
        FixSizedTypesCodec.encode_int(initial_frame_buf, _METRIC_ENCODE_OFFSET, vector_index_config.metric)
        FixSizedTypesCodec.encode_int(initial_frame_buf, _DIMENSION_ENCODE_OFFSET, vector_index_config.dimension)
        FixSizedTypesCodec.encode_int(initial_frame_buf, _MAX_DEGREE_ENCODE_OFFSET, vector_index_config.max_degree)
        FixSizedTypesCodec.encode_int(initial_frame_buf, _EF_CONSTRUCTION_ENCODE_OFFSET, vector_index_config.ef_construction)
        FixSizedTypesCodec.encode_boolean(initial_frame_buf, _USE_DEDUPLICATION_ENCODE_OFFSET, vector_index_config.use_deduplication)
        buf.extend(initial_frame_buf)
        CodecUtil.encode_nullable(buf, vector_index_config.name, StringCodec.encode)
        if is_final:
            buf.extend(END_FINAL_FRAME_BUF)
        else:
            buf.extend(END_FRAME_BUF)

    @staticmethod
    def decode(msg):
        msg.next_frame()
        initial_frame = msg.next_frame()
        metric = FixSizedTypesCodec.decode_int(initial_frame.buf, _METRIC_DECODE_OFFSET)
        dimension = FixSizedTypesCodec.decode_int(initial_frame.buf, _DIMENSION_DECODE_OFFSET)
        max_degree = FixSizedTypesCodec.decode_int(initial_frame.buf, _MAX_DEGREE_DECODE_OFFSET)
        ef_construction = FixSizedTypesCodec.decode_int(initial_frame.buf, _EF_CONSTRUCTION_DECODE_OFFSET)
        use_deduplication = FixSizedTypesCodec.decode_boolean(initial_frame.buf, _USE_DEDUPLICATION_DECODE_OFFSET)
        name = CodecUtil.decode_nullable(msg, StringCodec.decode)
        CodecUtil.fast_forward_to_end_frame(msg)
        return VectorIndexConfig(name, metric, dimension, max_degree, ef_construction, use_deduplication)
