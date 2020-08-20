from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer, RESPONSE_HEADER_SIZE

# hex: 0x080300
_REQUEST_MESSAGE_TYPE = 525056
# hex: 0x080301
_RESPONSE_MESSAGE_TYPE = 525057

_REQUEST_UUID_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_INTERRUPT_OFFSET = _REQUEST_UUID_OFFSET + UUID_SIZE_IN_BYTES
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_INTERRUPT_OFFSET + BOOLEAN_SIZE_IN_BYTES
_RESPONSE_RESPONSE_OFFSET = RESPONSE_HEADER_SIZE


def encode_request(uuid, interrupt):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE, True)
    FixSizedTypesCodec.encode_uuid(buf, _REQUEST_UUID_OFFSET, uuid)
    FixSizedTypesCodec.encode_boolean(buf, _REQUEST_INTERRUPT_OFFSET, interrupt)
    return OutboundMessage(buf, False)


def decode_response(msg):
    initial_frame = msg.next_frame()
    return FixSizedTypesCodec.decode_boolean(initial_frame.buf, _RESPONSE_RESPONSE_OFFSET)
