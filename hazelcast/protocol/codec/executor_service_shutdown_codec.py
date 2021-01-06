from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.builtin import StringCodec

# hex: 0x080100
_REQUEST_MESSAGE_TYPE = 524544
# hex: 0x080101
_RESPONSE_MESSAGE_TYPE = 524545

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(name):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    StringCodec.encode(buf, name, True)
    return OutboundMessage(buf, False)
