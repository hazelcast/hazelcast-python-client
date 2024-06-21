from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import ListMultiFrameCodec
from hazelcast.protocol.codec.custom.vector_pair_codec import VectorPairCodec
from hazelcast.protocol.codec.custom.vector_search_options_codec import VectorSearchOptionsCodec
from hazelcast.protocol.codec.custom.vector_search_result_codec import VectorSearchResultCodec

# hex: 0x240800
_REQUEST_MESSAGE_TYPE = 2361344
# hex: 0x240801
_RESPONSE_MESSAGE_TYPE = 2361345

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(name, vectors, options):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    StringCodec.encode(buf, name)
    ListMultiFrameCodec.encode(buf, vectors, VectorPairCodec.encode)
    VectorSearchOptionsCodec.encode(buf, options, True)
    return OutboundMessage(buf, True)


def decode_response(msg):
    msg.next_frame()
    return ListMultiFrameCodec.decode(msg, VectorSearchResultCodec.decode)
