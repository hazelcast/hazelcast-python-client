from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.codec.custom.raft_group_id_codec import RaftGroupIdCodec

# hex: 0x1E0100
_REQUEST_MESSAGE_TYPE = 1966336
# hex: 0x1E0101
_RESPONSE_MESSAGE_TYPE = 1966337

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(proxy_name):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    StringCodec.encode(buf, proxy_name, True)
    return OutboundMessage(buf, True)


def decode_response(msg):
    msg.next_frame()
    return RaftGroupIdCodec.decode(msg)
