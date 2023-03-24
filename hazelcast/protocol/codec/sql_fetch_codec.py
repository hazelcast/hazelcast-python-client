from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.codec.custom.sql_query_id_codec import SqlQueryIdCodec
from hazelcast.protocol.builtin import SqlPageCodec
from hazelcast.protocol.builtin import CodecUtil
from hazelcast.protocol.codec.custom.sql_error_codec import SqlErrorCodec

# hex: 0x210500
_REQUEST_MESSAGE_TYPE = 2163968
# hex: 0x210501
_RESPONSE_MESSAGE_TYPE = 2163969

_REQUEST_CURSOR_BUFFER_SIZE_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_CURSOR_BUFFER_SIZE_OFFSET + INT_SIZE_IN_BYTES


def encode_request(query_id, cursor_buffer_size):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    FixSizedTypesCodec.encode_int(buf, _REQUEST_CURSOR_BUFFER_SIZE_OFFSET, cursor_buffer_size)
    SqlQueryIdCodec.encode(buf, query_id, True)
    return OutboundMessage(buf, False)


def decode_response(msg, to_object_fn):
    msg.next_frame()
    response = dict()
    response["row_page"] = CodecUtil.decode_nullable(msg, lambda m: SqlPageCodec.decode(m, to_object_fn))
    response["error"] = CodecUtil.decode_nullable(msg, SqlErrorCodec.decode)
    return response
