from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import CodecUtil

# hex: 0x240900
_REQUEST_MESSAGE_TYPE = 2361600
# hex: 0x240901
_RESPONSE_MESSAGE_TYPE = 2361601

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(name, index_name):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    StringCodec.encode(buf, name)
    CodecUtil.encode_nullable(buf, index_name, StringCodec.encode, True)
    return OutboundMessage(buf, True)
