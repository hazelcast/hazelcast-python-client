from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import CodecUtil

# hex: 0x240900
_REQUEST_MESSAGE_TYPE = 2361600
# hex: 0x240901
_RESPONSE_MESSAGE_TYPE = 2361601

_REQUEST_UUID_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_UUID_OFFSET + UUID_SIZE_IN_BYTES


def encode_request(name, index_name, uuid):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    FixSizedTypesCodec.encode_uuid(buf, _REQUEST_UUID_OFFSET, uuid)
    StringCodec.encode(buf, name)
    CodecUtil.encode_nullable(buf, index_name, StringCodec.encode, True)
    return OutboundMessage(buf, True)
