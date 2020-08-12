from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import ListMultiFrameCodec
from hazelcast.protocol.builtin import DataCodec

# hex: 0x0D0F00
_REQUEST_MESSAGE_TYPE = 855808
# hex: 0x0D0F01
_RESPONSE_MESSAGE_TYPE = 855809

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(name):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    StringCodec.encode(buf, name)
    return OutboundMessage(buf, True)


def decode_response(msg):
    msg.next_frame()
    return ListMultiFrameCodec.decode(msg, DataCodec.decode)
