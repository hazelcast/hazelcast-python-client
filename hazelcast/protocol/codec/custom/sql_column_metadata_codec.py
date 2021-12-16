from hazelcast.protocol.builtin import FixSizedTypesCodec, CodecUtil
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import END_FRAME_BUF, END_FINAL_FRAME_BUF, SIZE_OF_FRAME_LENGTH_AND_FLAGS, create_initial_buffer_custom
from hazelcast.sql import SqlColumnMetadata
from hazelcast.protocol.builtin import StringCodec

_TYPE_ENCODE_OFFSET = 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS
_TYPE_DECODE_OFFSET = 0
_NULLABLE_ENCODE_OFFSET = _TYPE_ENCODE_OFFSET + INT_SIZE_IN_BYTES
_NULLABLE_DECODE_OFFSET = _TYPE_DECODE_OFFSET + INT_SIZE_IN_BYTES
_INITIAL_FRAME_SIZE = _NULLABLE_ENCODE_OFFSET + BOOLEAN_SIZE_IN_BYTES - SIZE_OF_FRAME_LENGTH_AND_FLAGS


class SqlColumnMetadataCodec:
    @staticmethod
    def encode(buf, sql_column_metadata, is_final=False):
        initial_frame_buf = create_initial_buffer_custom(_INITIAL_FRAME_SIZE)
        FixSizedTypesCodec.encode_int(initial_frame_buf, _TYPE_ENCODE_OFFSET, sql_column_metadata.type)
        FixSizedTypesCodec.encode_boolean(initial_frame_buf, _NULLABLE_ENCODE_OFFSET, sql_column_metadata.nullable)
        buf.extend(initial_frame_buf)
        StringCodec.encode(buf, sql_column_metadata.name)
        if is_final:
            buf.extend(END_FINAL_FRAME_BUF)
        else:
            buf.extend(END_FRAME_BUF)

    @staticmethod
    def decode(msg):
        msg.next_frame()
        initial_frame = msg.next_frame()
        type = FixSizedTypesCodec.decode_int(initial_frame.buf, _TYPE_DECODE_OFFSET)
        is_nullable_exists = False
        nullable = False
        if len(initial_frame.buf) >= _NULLABLE_DECODE_OFFSET + BOOLEAN_SIZE_IN_BYTES:
            nullable = FixSizedTypesCodec.decode_boolean(initial_frame.buf, _NULLABLE_DECODE_OFFSET)
            is_nullable_exists = True
        name = StringCodec.decode(msg)
        CodecUtil.fast_forward_to_end_frame(msg)
        return SqlColumnMetadata(name, type, is_nullable_exists, nullable)
