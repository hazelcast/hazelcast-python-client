from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer, RESPONSE_HEADER_SIZE
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import ListMultiFrameCodec
from hazelcast.protocol.builtin import DataCodec
from hazelcast.protocol.builtin import CodecUtil
from hazelcast.protocol.codec.custom.sql_query_id_codec import SqlQueryIdCodec
from hazelcast.protocol.codec.custom.sql_column_metadata_codec import SqlColumnMetadataCodec
from hazelcast.protocol.builtin import SqlPageCodec
from hazelcast.protocol.codec.custom.sql_error_codec import SqlErrorCodec

# hex: 0x210400
_REQUEST_MESSAGE_TYPE = 2163712
# hex: 0x210401
_RESPONSE_MESSAGE_TYPE = 2163713

_REQUEST_TIMEOUT_MILLIS_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_CURSOR_BUFFER_SIZE_OFFSET = _REQUEST_TIMEOUT_MILLIS_OFFSET + LONG_SIZE_IN_BYTES
_REQUEST_EXPECTED_RESULT_TYPE_OFFSET = _REQUEST_CURSOR_BUFFER_SIZE_OFFSET + INT_SIZE_IN_BYTES
_REQUEST_SKIP_UPDATE_STATISTICS_OFFSET = _REQUEST_EXPECTED_RESULT_TYPE_OFFSET + BYTE_SIZE_IN_BYTES
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_SKIP_UPDATE_STATISTICS_OFFSET + BOOLEAN_SIZE_IN_BYTES
_RESPONSE_UPDATE_COUNT_OFFSET = RESPONSE_HEADER_SIZE


def encode_request(sql, parameters, timeout_millis, cursor_buffer_size, schema, expected_result_type, query_id, skip_update_statistics):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    FixSizedTypesCodec.encode_long(buf, _REQUEST_TIMEOUT_MILLIS_OFFSET, timeout_millis)
    FixSizedTypesCodec.encode_int(buf, _REQUEST_CURSOR_BUFFER_SIZE_OFFSET, cursor_buffer_size)
    FixSizedTypesCodec.encode_byte(buf, _REQUEST_EXPECTED_RESULT_TYPE_OFFSET, expected_result_type)
    FixSizedTypesCodec.encode_boolean(buf, _REQUEST_SKIP_UPDATE_STATISTICS_OFFSET, skip_update_statistics)
    StringCodec.encode(buf, sql)
    ListMultiFrameCodec.encode_contains_nullable(buf, parameters, DataCodec.encode)
    CodecUtil.encode_nullable(buf, schema, StringCodec.encode)
    SqlQueryIdCodec.encode(buf, query_id, True)
    return OutboundMessage(buf, False)


def decode_response(msg):
    initial_frame = msg.next_frame()
    response = dict()
    response["update_count"] = FixSizedTypesCodec.decode_long(initial_frame.buf, _RESPONSE_UPDATE_COUNT_OFFSET)
    response["row_metadata"] = ListMultiFrameCodec.decode_nullable(msg, SqlColumnMetadataCodec.decode)
    response["row_page"] = CodecUtil.decode_nullable(msg, SqlPageCodec.decode)
    response["error"] = CodecUtil.decode_nullable(msg, SqlErrorCodec.decode)
    return response
