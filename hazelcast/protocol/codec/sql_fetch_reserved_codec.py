from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer, RESPONSE_HEADER_SIZE
from hazelcast.protocol.codec.custom.sql_query_id_codec import SqlQueryIdCodec
from hazelcast.protocol.builtin import ListMultiFrameCodec
from hazelcast.protocol.builtin import ListCNDataCodec
from hazelcast.protocol.codec.custom.sql_error_codec import SqlErrorCodec
from hazelcast.protocol.builtin import CodecUtil

# hex: 0x210200
_REQUEST_MESSAGE_TYPE = 2163200
# hex: 0x210201
_RESPONSE_MESSAGE_TYPE = 2163201

_REQUEST_CURSOR_BUFFER_SIZE_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_CURSOR_BUFFER_SIZE_OFFSET + INT_SIZE_IN_BYTES
_RESPONSE_ROW_PAGE_LAST_OFFSET = RESPONSE_HEADER_SIZE


def encode_request(query_id, cursor_buffer_size):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    FixSizedTypesCodec.encode_int(buf, _REQUEST_CURSOR_BUFFER_SIZE_OFFSET, cursor_buffer_size)
    SqlQueryIdCodec.encode(buf, query_id, True)
    return OutboundMessage(buf, False)


def decode_response(msg):
    initial_frame = msg.next_frame()
    response = dict()
    response["row_page_last"] = FixSizedTypesCodec.decode_boolean(initial_frame.buf, _RESPONSE_ROW_PAGE_LAST_OFFSET)
    response["row_page"] = ListMultiFrameCodec.decode_nullable(msg, ListCNDataCodec.decode)
    response["error"] = CodecUtil.decode_nullable(msg, SqlErrorCodec.decode)
    return response
