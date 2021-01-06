from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer, RESPONSE_HEADER_SIZE
from hazelcast.protocol.codec.custom.raft_group_id_codec import RaftGroupIdCodec
from hazelcast.protocol.builtin import StringCodec

# hex: 0x1F0100
_REQUEST_MESSAGE_TYPE = 2031872
# hex: 0x1F0101
_RESPONSE_MESSAGE_TYPE = 2031873

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE
_RESPONSE_SESSION_ID_OFFSET = RESPONSE_HEADER_SIZE
_RESPONSE_TTL_MILLIS_OFFSET = _RESPONSE_SESSION_ID_OFFSET + LONG_SIZE_IN_BYTES
_RESPONSE_HEARTBEAT_MILLIS_OFFSET = _RESPONSE_TTL_MILLIS_OFFSET + LONG_SIZE_IN_BYTES


def encode_request(group_id, endpoint_name):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    RaftGroupIdCodec.encode(buf, group_id)
    StringCodec.encode(buf, endpoint_name, True)
    return OutboundMessage(buf, True)


def decode_response(msg):
    initial_frame = msg.next_frame()
    response = dict()
    response["session_id"] = FixSizedTypesCodec.decode_long(initial_frame.buf, _RESPONSE_SESSION_ID_OFFSET)
    response["ttl_millis"] = FixSizedTypesCodec.decode_long(initial_frame.buf, _RESPONSE_TTL_MILLIS_OFFSET)
    response["heartbeat_millis"] = FixSizedTypesCodec.decode_long(initial_frame.buf, _RESPONSE_HEARTBEAT_MILLIS_OFFSET)
    return response
