from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer, RESPONSE_HEADER_SIZE, EVENT_HEADER_SIZE
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import DataCodec

# hex: 0x040200
_REQUEST_MESSAGE_TYPE = 262656
# hex: 0x040201
_RESPONSE_MESSAGE_TYPE = 262657
# hex: 0x040202
_EVENT_TOPIC_MESSAGE_TYPE = 262658

_REQUEST_LOCAL_ONLY_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_LOCAL_ONLY_OFFSET + BOOLEAN_SIZE_IN_BYTES
_RESPONSE_RESPONSE_OFFSET = RESPONSE_HEADER_SIZE
_EVENT_TOPIC_PUBLISH_TIME_OFFSET = EVENT_HEADER_SIZE
_EVENT_TOPIC_UUID_OFFSET = _EVENT_TOPIC_PUBLISH_TIME_OFFSET + LONG_SIZE_IN_BYTES


def encode_request(name, local_only):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    FixSizedTypesCodec.encode_boolean(buf, _REQUEST_LOCAL_ONLY_OFFSET, local_only)
    StringCodec.encode(buf, name, True)
    return OutboundMessage(buf, False)


def decode_response(msg):
    initial_frame = msg.next_frame()
    return FixSizedTypesCodec.decode_uuid(initial_frame.buf, _RESPONSE_RESPONSE_OFFSET)


def handle(msg, handle_topic_event=None):
    message_type = msg.get_message_type()
    if message_type == _EVENT_TOPIC_MESSAGE_TYPE and handle_topic_event is not None:
        initial_frame = msg.next_frame()
        publish_time = FixSizedTypesCodec.decode_long(initial_frame.buf, _EVENT_TOPIC_PUBLISH_TIME_OFFSET)
        uuid = FixSizedTypesCodec.decode_uuid(initial_frame.buf, _EVENT_TOPIC_UUID_OFFSET)
        item = DataCodec.decode(msg)
        handle_topic_event(item, publish_time, uuid)
        return
