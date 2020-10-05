from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer, RESPONSE_HEADER_SIZE, EVENT_HEADER_SIZE

# hex: 0x000600
_REQUEST_MESSAGE_TYPE = 1536
# hex: 0x000601
_RESPONSE_MESSAGE_TYPE = 1537
# hex: 0x000602
_EVENT_PARTITION_LOST_MESSAGE_TYPE = 1538

_REQUEST_LOCAL_ONLY_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_LOCAL_ONLY_OFFSET + BOOLEAN_SIZE_IN_BYTES
_RESPONSE_RESPONSE_OFFSET = RESPONSE_HEADER_SIZE
_EVENT_PARTITION_LOST_PARTITION_ID_OFFSET = EVENT_HEADER_SIZE
_EVENT_PARTITION_LOST_LOST_BACKUP_COUNT_OFFSET = _EVENT_PARTITION_LOST_PARTITION_ID_OFFSET + INT_SIZE_IN_BYTES
_EVENT_PARTITION_LOST_SOURCE_OFFSET = _EVENT_PARTITION_LOST_LOST_BACKUP_COUNT_OFFSET + INT_SIZE_IN_BYTES


def encode_request(local_only):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE, True)
    FixSizedTypesCodec.encode_boolean(buf, _REQUEST_LOCAL_ONLY_OFFSET, local_only)
    return OutboundMessage(buf, False)


def decode_response(msg):
    initial_frame = msg.next_frame()
    return FixSizedTypesCodec.decode_uuid(initial_frame.buf, _RESPONSE_RESPONSE_OFFSET)


def handle(msg, handle_partition_lost_event=None):
    message_type = msg.get_message_type()
    if message_type == _EVENT_PARTITION_LOST_MESSAGE_TYPE and handle_partition_lost_event is not None:
        initial_frame = msg.next_frame()
        partition_id = FixSizedTypesCodec.decode_int(initial_frame.buf, _EVENT_PARTITION_LOST_PARTITION_ID_OFFSET)
        lost_backup_count = FixSizedTypesCodec.decode_int(initial_frame.buf, _EVENT_PARTITION_LOST_LOST_BACKUP_COUNT_OFFSET)
        source = FixSizedTypesCodec.decode_uuid(initial_frame.buf, _EVENT_PARTITION_LOST_SOURCE_OFFSET)
        handle_partition_lost_event(partition_id, lost_backup_count, source)
        return
