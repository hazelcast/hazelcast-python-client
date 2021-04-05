from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.codec.custom.sql_query_id_codec import SqlQueryIdCodec

# hex: 0x210300
_REQUEST_MESSAGE_TYPE = 2163456
# hex: 0x210301
_RESPONSE_MESSAGE_TYPE = 2163457

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(query_id):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    SqlQueryIdCodec.encode(buf, query_id, True)
    return OutboundMessage(buf, False)
