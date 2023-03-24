from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import ListMultiFrameCodec
from hazelcast.protocol.builtin import DataCodec

# hex: 0x040400
_REQUEST_MESSAGE_TYPE = 263168
# hex: 0x040401
_RESPONSE_MESSAGE_TYPE = 263169

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(name, messages):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    StringCodec.encode(buf, name)
    ListMultiFrameCodec.encode(buf, messages, DataCodec.encode, True)
    return OutboundMessage(buf, False, True)
