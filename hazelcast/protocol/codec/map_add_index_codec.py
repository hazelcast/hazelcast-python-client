from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.codec.custom.index_config_codec import IndexConfigCodec

# hex: 0x012900
_REQUEST_MESSAGE_TYPE = 76032
# hex: 0x012901
_RESPONSE_MESSAGE_TYPE = 76033

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(name, index_config):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    StringCodec.encode(buf, name)
    IndexConfigCodec.encode(buf, index_config, True)
    return OutboundMessage(buf, False)
