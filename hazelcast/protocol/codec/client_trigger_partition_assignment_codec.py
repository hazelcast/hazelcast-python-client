from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer

# hex: 0x001000
_REQUEST_MESSAGE_TYPE = 4096
# hex: 0x001001
_RESPONSE_MESSAGE_TYPE = 4097

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request():
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE, True)
    return OutboundMessage(buf, True)
