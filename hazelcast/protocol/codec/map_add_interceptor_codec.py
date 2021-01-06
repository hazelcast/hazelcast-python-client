from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import DataCodec

# hex: 0x011400
_REQUEST_MESSAGE_TYPE = 70656
# hex: 0x011401
_RESPONSE_MESSAGE_TYPE = 70657

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(name, interceptor):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    StringCodec.encode(buf, name)
    DataCodec.encode(buf, interceptor, True)
    return OutboundMessage(buf, False)


def decode_response(msg):
    msg.next_frame()
    return StringCodec.decode(msg)
