from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import ListMultiFrameCodec
from hazelcast.protocol.builtin import DataCodec

# hex: 0x030900
_REQUEST_MESSAGE_TYPE = 198912
# hex: 0x030901
_RESPONSE_MESSAGE_TYPE = 198913

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(name):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    StringCodec.encode(buf, name, True)
    return OutboundMessage(buf, False)


def decode_response(msg):
    msg.next_frame()
    return ListMultiFrameCodec.decode(msg, DataCodec.decode)
