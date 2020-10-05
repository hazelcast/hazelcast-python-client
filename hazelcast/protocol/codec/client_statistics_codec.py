from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import ByteArrayCodec

# hex: 0x000C00
_REQUEST_MESSAGE_TYPE = 3072
# hex: 0x000C01
_RESPONSE_MESSAGE_TYPE = 3073

_REQUEST_TIMESTAMP_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_TIMESTAMP_OFFSET + LONG_SIZE_IN_BYTES


def encode_request(timestamp, client_attributes, metrics_blob):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    FixSizedTypesCodec.encode_long(buf, _REQUEST_TIMESTAMP_OFFSET, timestamp)
    StringCodec.encode(buf, client_attributes)
    ByteArrayCodec.encode(buf, metrics_blob, True)
    return OutboundMessage(buf, False)
