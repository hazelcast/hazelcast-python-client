from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import EntryListCodec
from hazelcast.protocol.builtin import DataCodec

# hex: 0x012C00
_REQUEST_MESSAGE_TYPE = 76800
# hex: 0x012C01
_RESPONSE_MESSAGE_TYPE = 76801

_REQUEST_TRIGGER_MAP_LOADER_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_TRIGGER_MAP_LOADER_OFFSET + BOOLEAN_SIZE_IN_BYTES


def encode_request(name, entries, trigger_map_loader):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    FixSizedTypesCodec.encode_boolean(buf, _REQUEST_TRIGGER_MAP_LOADER_OFFSET, trigger_map_loader)
    StringCodec.encode(buf, name)
    EntryListCodec.encode(buf, entries, DataCodec.encode, DataCodec.encode, True)
    return OutboundMessage(buf, False)
