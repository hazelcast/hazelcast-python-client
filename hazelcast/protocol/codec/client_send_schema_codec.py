from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.codec.custom.schema_codec import SchemaCodec

# hex: 0x001300
_REQUEST_MESSAGE_TYPE = 4864
# hex: 0x001301
_RESPONSE_MESSAGE_TYPE = 4865

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(schema):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    SchemaCodec.encode(buf, schema, True)
    return OutboundMessage(buf, True)
