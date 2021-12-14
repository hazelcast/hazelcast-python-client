from hazelcast.protocol.builtin import FixSizedTypesCodec, CodecUtil
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import END_FRAME_BUF, END_FINAL_FRAME_BUF, SIZE_OF_FRAME_LENGTH_AND_FLAGS, create_initial_buffer_custom
from hazelcast.sql import _SqlQueryId

_MEMBER_ID_HIGH_ENCODE_OFFSET = 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS
_MEMBER_ID_HIGH_DECODE_OFFSET = 0
_MEMBER_ID_LOW_ENCODE_OFFSET = _MEMBER_ID_HIGH_ENCODE_OFFSET + LONG_SIZE_IN_BYTES
_MEMBER_ID_LOW_DECODE_OFFSET = _MEMBER_ID_HIGH_DECODE_OFFSET + LONG_SIZE_IN_BYTES
_LOCAL_ID_HIGH_ENCODE_OFFSET = _MEMBER_ID_LOW_ENCODE_OFFSET + LONG_SIZE_IN_BYTES
_LOCAL_ID_HIGH_DECODE_OFFSET = _MEMBER_ID_LOW_DECODE_OFFSET + LONG_SIZE_IN_BYTES
_LOCAL_ID_LOW_ENCODE_OFFSET = _LOCAL_ID_HIGH_ENCODE_OFFSET + LONG_SIZE_IN_BYTES
_LOCAL_ID_LOW_DECODE_OFFSET = _LOCAL_ID_HIGH_DECODE_OFFSET + LONG_SIZE_IN_BYTES
_INITIAL_FRAME_SIZE = _LOCAL_ID_LOW_ENCODE_OFFSET + LONG_SIZE_IN_BYTES - SIZE_OF_FRAME_LENGTH_AND_FLAGS


class SqlQueryIdCodec:
    @staticmethod
    def encode(buf, sql_query_id, is_final=False):
        initial_frame_buf = create_initial_buffer_custom(_INITIAL_FRAME_SIZE)
        FixSizedTypesCodec.encode_long(initial_frame_buf, _MEMBER_ID_HIGH_ENCODE_OFFSET, sql_query_id.member_id_high)
        FixSizedTypesCodec.encode_long(initial_frame_buf, _MEMBER_ID_LOW_ENCODE_OFFSET, sql_query_id.member_id_low)
        FixSizedTypesCodec.encode_long(initial_frame_buf, _LOCAL_ID_HIGH_ENCODE_OFFSET, sql_query_id.local_id_high)
        FixSizedTypesCodec.encode_long(initial_frame_buf, _LOCAL_ID_LOW_ENCODE_OFFSET, sql_query_id.local_id_low)
        buf.extend(initial_frame_buf)
        if is_final:
            buf.extend(END_FINAL_FRAME_BUF)
        else:
            buf.extend(END_FRAME_BUF)

    @staticmethod
    def decode(msg):
        msg.next_frame()
        initial_frame = msg.next_frame()
        member_id_high = FixSizedTypesCodec.decode_long(initial_frame.buf, _MEMBER_ID_HIGH_DECODE_OFFSET)
        member_id_low = FixSizedTypesCodec.decode_long(initial_frame.buf, _MEMBER_ID_LOW_DECODE_OFFSET)
        local_id_high = FixSizedTypesCodec.decode_long(initial_frame.buf, _LOCAL_ID_HIGH_DECODE_OFFSET)
        local_id_low = FixSizedTypesCodec.decode_long(initial_frame.buf, _LOCAL_ID_LOW_DECODE_OFFSET)
        CodecUtil.fast_forward_to_end_frame(msg)
        return _SqlQueryId(member_id_high, member_id_low, local_id_high, local_id_low)
