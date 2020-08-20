from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer, RESPONSE_HEADER_SIZE
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import EntryListUUIDLongCodec

# hex: 0x1D0200
_REQUEST_MESSAGE_TYPE = 1901056
# hex: 0x1D0201
_RESPONSE_MESSAGE_TYPE = 1901057

_REQUEST_DELTA_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_GET_BEFORE_UPDATE_OFFSET = _REQUEST_DELTA_OFFSET + LONG_SIZE_IN_BYTES
_REQUEST_TARGET_REPLICA_UUID_OFFSET = _REQUEST_GET_BEFORE_UPDATE_OFFSET + BOOLEAN_SIZE_IN_BYTES
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_TARGET_REPLICA_UUID_OFFSET + UUID_SIZE_IN_BYTES
_RESPONSE_VALUE_OFFSET = RESPONSE_HEADER_SIZE
_RESPONSE_REPLICA_COUNT_OFFSET = _RESPONSE_VALUE_OFFSET + LONG_SIZE_IN_BYTES


def encode_request(name, delta, get_before_update, replica_timestamps, target_replica_uuid):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    FixSizedTypesCodec.encode_long(buf, _REQUEST_DELTA_OFFSET, delta)
    FixSizedTypesCodec.encode_boolean(buf, _REQUEST_GET_BEFORE_UPDATE_OFFSET, get_before_update)
    FixSizedTypesCodec.encode_uuid(buf, _REQUEST_TARGET_REPLICA_UUID_OFFSET, target_replica_uuid)
    StringCodec.encode(buf, name)
    EntryListUUIDLongCodec.encode(buf, replica_timestamps, True)
    return OutboundMessage(buf, False)


def decode_response(msg):
    initial_frame = msg.next_frame()
    response = dict()
    response["value"] = FixSizedTypesCodec.decode_long(initial_frame.buf, _RESPONSE_VALUE_OFFSET)
    response["replica_count"] = FixSizedTypesCodec.decode_int(initial_frame.buf, _RESPONSE_REPLICA_COUNT_OFFSET)
    response["replica_timestamps"] = EntryListUUIDLongCodec.decode(msg)
    return response
