from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import DataCodec
from hazelcast.protocol.builtin import EntryListCodec

# hex: 0x013000
_REQUEST_MESSAGE_TYPE = 77824
# hex: 0x013001
_RESPONSE_MESSAGE_TYPE = 77825

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(name, entry_processor):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    StringCodec.encode(buf, name)
    DataCodec.encode(buf, entry_processor, True)
    return OutboundMessage(buf, False)


def decode_response(msg):
    msg.next_frame()
    return EntryListCodec.decode(msg, DataCodec.decode, DataCodec.decode)
