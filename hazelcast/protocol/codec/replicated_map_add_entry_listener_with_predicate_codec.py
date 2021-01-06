from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer, RESPONSE_HEADER_SIZE, EVENT_HEADER_SIZE
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import DataCodec
from hazelcast.protocol.builtin import CodecUtil

# hex: 0x0D0B00
_REQUEST_MESSAGE_TYPE = 854784
# hex: 0x0D0B01
_RESPONSE_MESSAGE_TYPE = 854785
# hex: 0x0D0B02
_EVENT_ENTRY_MESSAGE_TYPE = 854786

_REQUEST_LOCAL_ONLY_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_LOCAL_ONLY_OFFSET + BOOLEAN_SIZE_IN_BYTES
_RESPONSE_RESPONSE_OFFSET = RESPONSE_HEADER_SIZE
_EVENT_ENTRY_EVENT_TYPE_OFFSET = EVENT_HEADER_SIZE
_EVENT_ENTRY_UUID_OFFSET = _EVENT_ENTRY_EVENT_TYPE_OFFSET + INT_SIZE_IN_BYTES
_EVENT_ENTRY_NUMBER_OF_AFFECTED_ENTRIES_OFFSET = _EVENT_ENTRY_UUID_OFFSET + UUID_SIZE_IN_BYTES


def encode_request(name, predicate, local_only):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    FixSizedTypesCodec.encode_boolean(buf, _REQUEST_LOCAL_ONLY_OFFSET, local_only)
    StringCodec.encode(buf, name)
    DataCodec.encode(buf, predicate, True)
    return OutboundMessage(buf, False)


def decode_response(msg):
    initial_frame = msg.next_frame()
    return FixSizedTypesCodec.decode_uuid(initial_frame.buf, _RESPONSE_RESPONSE_OFFSET)


def handle(msg, handle_entry_event=None):
    message_type = msg.get_message_type()
    if message_type == _EVENT_ENTRY_MESSAGE_TYPE and handle_entry_event is not None:
        initial_frame = msg.next_frame()
        event_type = FixSizedTypesCodec.decode_int(initial_frame.buf, _EVENT_ENTRY_EVENT_TYPE_OFFSET)
        uuid = FixSizedTypesCodec.decode_uuid(initial_frame.buf, _EVENT_ENTRY_UUID_OFFSET)
        number_of_affected_entries = FixSizedTypesCodec.decode_int(initial_frame.buf, _EVENT_ENTRY_NUMBER_OF_AFFECTED_ENTRIES_OFFSET)
        key = CodecUtil.decode_nullable(msg, DataCodec.decode)
        value = CodecUtil.decode_nullable(msg, DataCodec.decode)
        old_value = CodecUtil.decode_nullable(msg, DataCodec.decode)
        merging_value = CodecUtil.decode_nullable(msg, DataCodec.decode)
        handle_entry_event(key, value, old_value, merging_value, event_type, uuid, number_of_affected_entries)
        return
