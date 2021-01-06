from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import ListMultiFrameCodec
from hazelcast.protocol.builtin import DataCodec

# hex: 0x051500
_REQUEST_MESSAGE_TYPE = 333056
# hex: 0x051501
_RESPONSE_MESSAGE_TYPE = 333057

_REQUEST_FROM_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_TO_OFFSET = _REQUEST_FROM_OFFSET + INT_SIZE_IN_BYTES
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_TO_OFFSET + INT_SIZE_IN_BYTES


def encode_request(name, _from, to):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    FixSizedTypesCodec.encode_int(buf, _REQUEST_FROM_OFFSET, _from)
    FixSizedTypesCodec.encode_int(buf, _REQUEST_TO_OFFSET, to)
    StringCodec.encode(buf, name, True)
    return OutboundMessage(buf, True)


def decode_response(msg):
    msg.next_frame()
    return ListMultiFrameCodec.decode(msg, DataCodec.decode)
