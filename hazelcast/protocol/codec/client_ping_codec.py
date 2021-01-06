from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer

# hex: 0x000B00
_REQUEST_MESSAGE_TYPE = 2816
# hex: 0x000B01
_RESPONSE_MESSAGE_TYPE = 2817

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request():
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE, True)
    return OutboundMessage(buf, True)
