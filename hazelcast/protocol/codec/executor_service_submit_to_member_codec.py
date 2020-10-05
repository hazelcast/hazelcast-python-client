from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import DataCodec
from hazelcast.protocol.builtin import CodecUtil

# hex: 0x080600
_REQUEST_MESSAGE_TYPE = 525824
# hex: 0x080601
_RESPONSE_MESSAGE_TYPE = 525825

_REQUEST_UUID_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_MEMBER_UUID_OFFSET = _REQUEST_UUID_OFFSET + UUID_SIZE_IN_BYTES
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_MEMBER_UUID_OFFSET + UUID_SIZE_IN_BYTES


def encode_request(name, uuid, callable, member_uuid):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    FixSizedTypesCodec.encode_uuid(buf, _REQUEST_UUID_OFFSET, uuid)
    FixSizedTypesCodec.encode_uuid(buf, _REQUEST_MEMBER_UUID_OFFSET, member_uuid)
    StringCodec.encode(buf, name)
    DataCodec.encode(buf, callable, True)
    return OutboundMessage(buf, False)


def decode_response(msg):
    msg.next_frame()
    return CodecUtil.decode_nullable(msg, DataCodec.decode)
