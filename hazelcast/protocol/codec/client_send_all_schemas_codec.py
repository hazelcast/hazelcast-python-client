from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.builtin import ListMultiFrameCodec
from hazelcast.protocol.codec.custom.schema_codec import SchemaCodec

# hex: 0x001500
_REQUEST_MESSAGE_TYPE = 5376
# hex: 0x001501
_RESPONSE_MESSAGE_TYPE = 5377

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(schemas):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    ListMultiFrameCodec.encode(buf, schemas, SchemaCodec.encode, True)
    return OutboundMessage(buf, True)
