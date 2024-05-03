from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import DataCodec
from hazelcast.protocol.codec.custom.vector_document_codec import VectorDocumentCodec

# hex: 0x240600
_REQUEST_MESSAGE_TYPE = 2360832
# hex: 0x240601
_RESPONSE_MESSAGE_TYPE = 2360833

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(name, key, value):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    StringCodec.encode(buf, name)
    DataCodec.encode(buf, key)
    VectorDocumentCodec.encode(buf, value, True)
    return OutboundMessage(buf, False, True)
