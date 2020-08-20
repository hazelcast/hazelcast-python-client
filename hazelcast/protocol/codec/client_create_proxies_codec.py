from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.builtin import EntryListCodec
from hazelcast.protocol.builtin import StringCodec

# hex: 0x000E00
_REQUEST_MESSAGE_TYPE = 3584
# hex: 0x000E01
_RESPONSE_MESSAGE_TYPE = 3585

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(proxies):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    EntryListCodec.encode(buf, proxies, StringCodec.encode, StringCodec.encode, True)
    return OutboundMessage(buf, False)
