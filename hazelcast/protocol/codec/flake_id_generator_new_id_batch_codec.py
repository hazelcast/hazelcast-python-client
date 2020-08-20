from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer, RESPONSE_HEADER_SIZE
from hazelcast.protocol.builtin import StringCodec

# hex: 0x1C0100
_REQUEST_MESSAGE_TYPE = 1835264
# hex: 0x1C0101
_RESPONSE_MESSAGE_TYPE = 1835265

_REQUEST_BATCH_SIZE_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_BATCH_SIZE_OFFSET + INT_SIZE_IN_BYTES
_RESPONSE_BASE_OFFSET = RESPONSE_HEADER_SIZE
_RESPONSE_INCREMENT_OFFSET = _RESPONSE_BASE_OFFSET + LONG_SIZE_IN_BYTES
_RESPONSE_BATCH_SIZE_OFFSET = _RESPONSE_INCREMENT_OFFSET + LONG_SIZE_IN_BYTES


def encode_request(name, batch_size):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    FixSizedTypesCodec.encode_int(buf, _REQUEST_BATCH_SIZE_OFFSET, batch_size)
    StringCodec.encode(buf, name, True)
    return OutboundMessage(buf, True)


def decode_response(msg):
    initial_frame = msg.next_frame()
    response = dict()
    response["base"] = FixSizedTypesCodec.decode_long(initial_frame.buf, _RESPONSE_BASE_OFFSET)
    response["increment"] = FixSizedTypesCodec.decode_long(initial_frame.buf, _RESPONSE_INCREMENT_OFFSET)
    response["batch_size"] = FixSizedTypesCodec.decode_int(initial_frame.buf, _RESPONSE_BATCH_SIZE_OFFSET)
    return response
