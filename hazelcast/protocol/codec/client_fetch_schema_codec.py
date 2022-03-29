from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.codec.custom.schema_codec import SchemaCodec
from hazelcast.protocol.builtin import CodecUtil

# hex: 0x001400
_REQUEST_MESSAGE_TYPE = 5120
# hex: 0x001401
_RESPONSE_MESSAGE_TYPE = 5121

_REQUEST_SCHEMA_ID_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_SCHEMA_ID_OFFSET + LONG_SIZE_IN_BYTES


def encode_request(schema_id):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE, True)
    FixSizedTypesCodec.encode_long(buf, _REQUEST_SCHEMA_ID_OFFSET, schema_id)
    return OutboundMessage(buf, True)


def decode_response(msg):
    msg.next_frame()
    return CodecUtil.decode_nullable(msg, SchemaCodec.decode)
