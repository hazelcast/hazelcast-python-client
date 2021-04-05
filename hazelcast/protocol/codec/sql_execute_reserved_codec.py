from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer, RESPONSE_HEADER_SIZE
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import ListMultiFrameCodec
from hazelcast.protocol.builtin import DataCodec
from hazelcast.protocol.codec.custom.sql_query_id_codec import SqlQueryIdCodec
from hazelcast.protocol.builtin import CodecUtil
from hazelcast.protocol.codec.custom.sql_column_metadata_codec import SqlColumnMetadataCodec
from hazelcast.protocol.builtin import ListCNDataCodec
from hazelcast.protocol.codec.custom.sql_error_codec import SqlErrorCodec

# hex: 0x210100
_REQUEST_MESSAGE_TYPE = 2162944
# hex: 0x210101
_RESPONSE_MESSAGE_TYPE = 2162945

_REQUEST_TIMEOUT_MILLIS_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_CURSOR_BUFFER_SIZE_OFFSET = _REQUEST_TIMEOUT_MILLIS_OFFSET + LONG_SIZE_IN_BYTES
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_CURSOR_BUFFER_SIZE_OFFSET + INT_SIZE_IN_BYTES
_RESPONSE_ROW_PAGE_LAST_OFFSET = RESPONSE_HEADER_SIZE
_RESPONSE_UPDATE_COUNT_OFFSET = _RESPONSE_ROW_PAGE_LAST_OFFSET + BOOLEAN_SIZE_IN_BYTES


def encode_request(sql, parameters, timeout_millis, cursor_buffer_size):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    FixSizedTypesCodec.encode_long(buf, _REQUEST_TIMEOUT_MILLIS_OFFSET, timeout_millis)
    FixSizedTypesCodec.encode_int(buf, _REQUEST_CURSOR_BUFFER_SIZE_OFFSET, cursor_buffer_size)
    StringCodec.encode(buf, sql)
    ListMultiFrameCodec.encode(buf, parameters, DataCodec.encode, True)
    return OutboundMessage(buf, False)


def decode_response(msg):
    initial_frame = msg.next_frame()
    response = dict()
    response["row_page_last"] = FixSizedTypesCodec.decode_boolean(initial_frame.buf, _RESPONSE_ROW_PAGE_LAST_OFFSET)
    response["update_count"] = FixSizedTypesCodec.decode_long(initial_frame.buf, _RESPONSE_UPDATE_COUNT_OFFSET)
    response["query_id"] = CodecUtil.decode_nullable(msg, SqlQueryIdCodec.decode)
    response["row_metadata"] = ListMultiFrameCodec.decode_nullable(msg, SqlColumnMetadataCodec.decode)
    response["row_page"] = ListMultiFrameCodec.decode_nullable(msg, ListCNDataCodec.decode)
    response["error"] = CodecUtil.decode_nullable(msg, SqlErrorCodec.decode)
    return response
