from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import DataCodec
from hazelcast.protocol.builtin import EntryListCodec

# hex: 0x012800
_REQUEST_MESSAGE_TYPE = 75776
# hex: 0x012801
_RESPONSE_MESSAGE_TYPE = 75777

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(name, predicate):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    StringCodec.encode(buf, name)
    DataCodec.encode(buf, predicate, True)
    return OutboundMessage(buf, True)


def decode_response(msg):
    msg.next_frame()
    return EntryListCodec.decode(msg, DataCodec.decode, DataCodec.decode)
