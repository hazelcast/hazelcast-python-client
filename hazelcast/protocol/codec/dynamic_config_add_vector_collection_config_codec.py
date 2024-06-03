from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import ListMultiFrameCodec
from hazelcast.protocol.codec.custom.vector_index_config_codec import VectorIndexConfigCodec

# hex: 0x1B1400
_REQUEST_MESSAGE_TYPE = 1774592
# hex: 0x1B1401
_RESPONSE_MESSAGE_TYPE = 1774593

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(name, index_configs):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    StringCodec.encode(buf, name)
    ListMultiFrameCodec.encode(buf, index_configs, VectorIndexConfigCodec.encode, True)
    return OutboundMessage(buf, False)
