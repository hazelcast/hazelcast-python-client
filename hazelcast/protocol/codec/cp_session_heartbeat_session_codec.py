from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.codec.custom.raft_group_id_codec import RaftGroupIdCodec

# hex: 0x1F0300
_REQUEST_MESSAGE_TYPE = 2032384
# hex: 0x1F0301
_RESPONSE_MESSAGE_TYPE = 2032385

_REQUEST_SESSION_ID_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_SESSION_ID_OFFSET + LONG_SIZE_IN_BYTES


def encode_request(group_id, session_id):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    FixSizedTypesCodec.encode_long(buf, _REQUEST_SESSION_ID_OFFSET, session_id)
    RaftGroupIdCodec.encode(buf, group_id, True)
    return OutboundMessage(buf, True)
