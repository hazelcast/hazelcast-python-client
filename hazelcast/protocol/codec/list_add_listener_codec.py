from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer, RESPONSE_HEADER_SIZE, EVENT_HEADER_SIZE
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import DataCodec
from hazelcast.protocol.builtin import CodecUtil

# hex: 0x050B00
_REQUEST_MESSAGE_TYPE = 330496
# hex: 0x050B01
_RESPONSE_MESSAGE_TYPE = 330497
# hex: 0x050B02
_EVENT_ITEM_MESSAGE_TYPE = 330498

_REQUEST_INCLUDE_VALUE_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_LOCAL_ONLY_OFFSET = _REQUEST_INCLUDE_VALUE_OFFSET + BOOLEAN_SIZE_IN_BYTES
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_LOCAL_ONLY_OFFSET + BOOLEAN_SIZE_IN_BYTES
_RESPONSE_RESPONSE_OFFSET = RESPONSE_HEADER_SIZE
_EVENT_ITEM_UUID_OFFSET = EVENT_HEADER_SIZE
_EVENT_ITEM_EVENT_TYPE_OFFSET = _EVENT_ITEM_UUID_OFFSET + UUID_SIZE_IN_BYTES


def encode_request(name, include_value, local_only):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    FixSizedTypesCodec.encode_boolean(buf, _REQUEST_INCLUDE_VALUE_OFFSET, include_value)
    FixSizedTypesCodec.encode_boolean(buf, _REQUEST_LOCAL_ONLY_OFFSET, local_only)
    StringCodec.encode(buf, name, True)
    return OutboundMessage(buf, False)


def decode_response(msg):
    initial_frame = msg.next_frame()
    return FixSizedTypesCodec.decode_uuid(initial_frame.buf, _RESPONSE_RESPONSE_OFFSET)


def handle(msg, handle_item_event=None):
    message_type = msg.get_message_type()
    if message_type == _EVENT_ITEM_MESSAGE_TYPE and handle_item_event is not None:
        initial_frame = msg.next_frame()
        uuid = FixSizedTypesCodec.decode_uuid(initial_frame.buf, _EVENT_ITEM_UUID_OFFSET)
        event_type = FixSizedTypesCodec.decode_int(initial_frame.buf, _EVENT_ITEM_EVENT_TYPE_OFFSET)
        item = CodecUtil.decode_nullable(msg, DataCodec.decode)
        handle_item_event(item, uuid, event_type)
        return
