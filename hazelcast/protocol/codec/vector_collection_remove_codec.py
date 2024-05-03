from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import DataCodec
from hazelcast.protocol.codec.custom.vector_document_codec import VectorDocumentCodec
from hazelcast.protocol.builtin import CodecUtil

# hex: 0x240500
_REQUEST_MESSAGE_TYPE = 2360576
# hex: 0x240501
_RESPONSE_MESSAGE_TYPE = 2360577

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(name, key):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    StringCodec.encode(buf, name)
    DataCodec.encode(buf, key, True)
    return OutboundMessage(buf, False, True)


def decode_response(msg):
    msg.next_frame()
    return CodecUtil.decode_nullable(msg, VectorDocumentCodec.decode)
