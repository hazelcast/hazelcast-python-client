from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import ListMultiFrameCodec
from hazelcast.protocol.builtin import DataCodec
from hazelcast.protocol.builtin import EntryListCodec

# hex: 0x012300
_REQUEST_MESSAGE_TYPE = 74496
# hex: 0x012301
_RESPONSE_MESSAGE_TYPE = 74497

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(name, keys):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    StringCodec.encode(buf, name)
    ListMultiFrameCodec.encode(buf, keys, DataCodec.encode, True)
    return OutboundMessage(buf, False)


def decode_response(msg):
    msg.next_frame()
    return EntryListCodec.decode(msg, DataCodec.decode, DataCodec.decode)
