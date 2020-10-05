from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer, RESPONSE_HEADER_SIZE, EVENT_HEADER_SIZE
from hazelcast.protocol.builtin import StringCodec

# hex: 0x000900
_REQUEST_MESSAGE_TYPE = 2304
# hex: 0x000901
_RESPONSE_MESSAGE_TYPE = 2305
# hex: 0x000902
_EVENT_DISTRIBUTED_OBJECT_MESSAGE_TYPE = 2306

_REQUEST_LOCAL_ONLY_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_LOCAL_ONLY_OFFSET + BOOLEAN_SIZE_IN_BYTES
_RESPONSE_RESPONSE_OFFSET = RESPONSE_HEADER_SIZE
_EVENT_DISTRIBUTED_OBJECT_SOURCE_OFFSET = EVENT_HEADER_SIZE


def encode_request(local_only):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE, True)
    FixSizedTypesCodec.encode_boolean(buf, _REQUEST_LOCAL_ONLY_OFFSET, local_only)
    return OutboundMessage(buf, False)


def decode_response(msg):
    initial_frame = msg.next_frame()
    return FixSizedTypesCodec.decode_uuid(initial_frame.buf, _RESPONSE_RESPONSE_OFFSET)


def handle(msg, handle_distributed_object_event=None):
    message_type = msg.get_message_type()
    if message_type == _EVENT_DISTRIBUTED_OBJECT_MESSAGE_TYPE and handle_distributed_object_event is not None:
        initial_frame = msg.next_frame()
        source = FixSizedTypesCodec.decode_uuid(initial_frame.buf, _EVENT_DISTRIBUTED_OBJECT_SOURCE_OFFSET)
        name = StringCodec.decode(msg)
        service_name = StringCodec.decode(msg)
        event_type = StringCodec.decode(msg)
        handle_distributed_object_event(name, service_name, event_type, source)
        return
