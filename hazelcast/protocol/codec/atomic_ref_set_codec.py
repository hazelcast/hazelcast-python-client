from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.codec.custom.raft_group_id_codec import RaftGroupIdCodec
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import DataCodec
from hazelcast.protocol.builtin import CodecUtil

# hex: 0x0A0500
_REQUEST_MESSAGE_TYPE = 656640
# hex: 0x0A0501
_RESPONSE_MESSAGE_TYPE = 656641

_REQUEST_RETURN_OLD_VALUE_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_RETURN_OLD_VALUE_OFFSET + BOOLEAN_SIZE_IN_BYTES


def encode_request(group_id, name, new_value, return_old_value):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    FixSizedTypesCodec.encode_boolean(buf, _REQUEST_RETURN_OLD_VALUE_OFFSET, return_old_value)
    RaftGroupIdCodec.encode(buf, group_id)
    StringCodec.encode(buf, name)
    CodecUtil.encode_nullable(buf, new_value, DataCodec.encode, True)
    return OutboundMessage(buf, False)


def decode_response(msg):
    msg.next_frame()
    return CodecUtil.decode_nullable(msg, DataCodec.decode)
