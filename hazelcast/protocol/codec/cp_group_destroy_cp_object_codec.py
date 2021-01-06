from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.codec.custom.raft_group_id_codec import RaftGroupIdCodec
from hazelcast.protocol.builtin import StringCodec

# hex: 0x1E0200
_REQUEST_MESSAGE_TYPE = 1966592
# hex: 0x1E0201
_RESPONSE_MESSAGE_TYPE = 1966593

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(group_id, service_name, object_name):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    RaftGroupIdCodec.encode(buf, group_id)
    StringCodec.encode(buf, service_name)
    StringCodec.encode(buf, object_name, True)
    return OutboundMessage(buf, True)
