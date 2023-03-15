from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import DataCodec
from hazelcast.protocol.builtin import ListMultiFrameCodec

# hex: 0x013C00
_REQUEST_MESSAGE_TYPE = 80896
# hex: 0x013C01
_RESPONSE_MESSAGE_TYPE = 80897

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(name, projection, predicate):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    StringCodec.encode(buf, name)
    DataCodec.encode(buf, projection)
    DataCodec.encode(buf, predicate, True)
    return OutboundMessage(buf, True, True)


def decode_response(msg):
    msg.next_frame()
    return ListMultiFrameCodec.decode_contains_nullable(msg, DataCodec.decode)
