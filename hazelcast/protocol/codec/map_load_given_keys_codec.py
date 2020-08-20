from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import ListMultiFrameCodec
from hazelcast.protocol.builtin import DataCodec

# hex: 0x012100
_REQUEST_MESSAGE_TYPE = 73984
# hex: 0x012101
_RESPONSE_MESSAGE_TYPE = 73985

_REQUEST_REPLACE_EXISTING_VALUES_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_REPLACE_EXISTING_VALUES_OFFSET + BOOLEAN_SIZE_IN_BYTES


def encode_request(name, keys, replace_existing_values):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    FixSizedTypesCodec.encode_boolean(buf, _REQUEST_REPLACE_EXISTING_VALUES_OFFSET, replace_existing_values)
    StringCodec.encode(buf, name)
    ListMultiFrameCodec.encode(buf, keys, DataCodec.encode, True)
    return OutboundMessage(buf, False)
