from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer, RESPONSE_HEADER_SIZE
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import EntryListUUIDLongCodec

# hex: 0x1D0100
_REQUEST_MESSAGE_TYPE = 1900800
# hex: 0x1D0101
_RESPONSE_MESSAGE_TYPE = 1900801

_REQUEST_TARGET_REPLICA_UUID_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_TARGET_REPLICA_UUID_OFFSET + UUID_SIZE_IN_BYTES
_RESPONSE_VALUE_OFFSET = RESPONSE_HEADER_SIZE
_RESPONSE_REPLICA_COUNT_OFFSET = _RESPONSE_VALUE_OFFSET + LONG_SIZE_IN_BYTES


def encode_request(name, replica_timestamps, target_replica_uuid):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    FixSizedTypesCodec.encode_uuid(buf, _REQUEST_TARGET_REPLICA_UUID_OFFSET, target_replica_uuid)
    StringCodec.encode(buf, name)
    EntryListUUIDLongCodec.encode(buf, replica_timestamps, True)
    return OutboundMessage(buf, True)


def decode_response(msg):
    initial_frame = msg.next_frame()
    response = dict()
    response["value"] = FixSizedTypesCodec.decode_long(initial_frame.buf, _RESPONSE_VALUE_OFFSET)
    response["replica_count"] = FixSizedTypesCodec.decode_int(initial_frame.buf, _RESPONSE_REPLICA_COUNT_OFFSET)
    response["replica_timestamps"] = EntryListUUIDLongCodec.decode(msg)
    return response
