from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer, RESPONSE_HEADER_SIZE
from hazelcast.protocol.codec.custom.raft_group_id_codec import RaftGroupIdCodec
from hazelcast.protocol.builtin import StringCodec

# hex: 0x0C0600
_REQUEST_MESSAGE_TYPE = 787968
# hex: 0x0C0601
_RESPONSE_MESSAGE_TYPE = 787969

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE
_RESPONSE_RESPONSE_OFFSET = RESPONSE_HEADER_SIZE


def encode_request(group_id, name):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    RaftGroupIdCodec.encode(buf, group_id)
    StringCodec.encode(buf, name, True)
    return OutboundMessage(buf, True)


def decode_response(msg):
    initial_frame = msg.next_frame()
    return FixSizedTypesCodec.decode_int(initial_frame.buf, _RESPONSE_RESPONSE_OFFSET)
