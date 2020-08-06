from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.builtin import StringCodec

# hex: 0x012D00
_REQUEST_MESSAGE_TYPE = 77056
# hex: 0x012D01
_RESPONSE_MESSAGE_TYPE = 77057

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(name):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    StringCodec.encode(buf, name)
    return OutboundMessage(buf, False)
