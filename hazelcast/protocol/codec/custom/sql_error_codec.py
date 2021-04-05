from hazelcast.protocol.builtin import FixSizedTypesCodec, CodecUtil
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import END_FRAME_BUF, END_FINAL_FRAME_BUF, SIZE_OF_FRAME_LENGTH_AND_FLAGS, create_initial_buffer_custom
from hazelcast.sql import _SqlError
from hazelcast.protocol.builtin import StringCodec

_CODE_ENCODE_OFFSET = 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS
_CODE_DECODE_OFFSET = 0
_ORIGINATING_MEMBER_ID_ENCODE_OFFSET = _CODE_ENCODE_OFFSET + INT_SIZE_IN_BYTES
_ORIGINATING_MEMBER_ID_DECODE_OFFSET = _CODE_DECODE_OFFSET + INT_SIZE_IN_BYTES
_INITIAL_FRAME_SIZE = _ORIGINATING_MEMBER_ID_ENCODE_OFFSET + UUID_SIZE_IN_BYTES - 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS


class SqlErrorCodec(object):
    @staticmethod
    def encode(buf, sql_error, is_final=False):
        initial_frame_buf = create_initial_buffer_custom(_INITIAL_FRAME_SIZE)
        FixSizedTypesCodec.encode_int(initial_frame_buf, _CODE_ENCODE_OFFSET, sql_error.code)
        FixSizedTypesCodec.encode_uuid(initial_frame_buf, _ORIGINATING_MEMBER_ID_ENCODE_OFFSET, sql_error.originating_member_id)
        buf.extend(initial_frame_buf)
        CodecUtil.encode_nullable(buf, sql_error.message, StringCodec.encode)
        if is_final:
            buf.extend(END_FINAL_FRAME_BUF)
        else:
            buf.extend(END_FRAME_BUF)

    @staticmethod
    def decode(msg):
        msg.next_frame()
        initial_frame = msg.next_frame()
        code = FixSizedTypesCodec.decode_int(initial_frame.buf, _CODE_DECODE_OFFSET)
        originating_member_id = FixSizedTypesCodec.decode_uuid(initial_frame.buf, _ORIGINATING_MEMBER_ID_DECODE_OFFSET)
        message = CodecUtil.decode_nullable(msg, StringCodec.decode)
        CodecUtil.fast_forward_to_end_frame(msg)
        return _SqlError(code, message, originating_member_id)
