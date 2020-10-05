from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer, EVENT_HEADER_SIZE
from hazelcast.protocol.builtin import ListMultiFrameCodec
from hazelcast.protocol.codec.custom.member_info_codec import MemberInfoCodec
from hazelcast.protocol.builtin import EntryListUUIDListIntegerCodec

# hex: 0x000300
_REQUEST_MESSAGE_TYPE = 768
# hex: 0x000301
_RESPONSE_MESSAGE_TYPE = 769
# hex: 0x000302
_EVENT_MEMBERS_VIEW_MESSAGE_TYPE = 770
# hex: 0x000303
_EVENT_PARTITIONS_VIEW_MESSAGE_TYPE = 771

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE
_EVENT_MEMBERS_VIEW_VERSION_OFFSET = EVENT_HEADER_SIZE
_EVENT_PARTITIONS_VIEW_VERSION_OFFSET = EVENT_HEADER_SIZE


def encode_request():
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE, True)
    return OutboundMessage(buf, False)


def handle(msg, handle_members_view_event=None, handle_partitions_view_event=None):
    message_type = msg.get_message_type()
    if message_type == _EVENT_MEMBERS_VIEW_MESSAGE_TYPE and handle_members_view_event is not None:
        initial_frame = msg.next_frame()
        version = FixSizedTypesCodec.decode_int(initial_frame.buf, _EVENT_MEMBERS_VIEW_VERSION_OFFSET)
        member_infos = ListMultiFrameCodec.decode(msg, MemberInfoCodec.decode)
        handle_members_view_event(version, member_infos)
        return
    if message_type == _EVENT_PARTITIONS_VIEW_MESSAGE_TYPE and handle_partitions_view_event is not None:
        initial_frame = msg.next_frame()
        version = FixSizedTypesCodec.decode_int(initial_frame.buf, _EVENT_PARTITIONS_VIEW_VERSION_OFFSET)
        partitions = EntryListUUIDListIntegerCodec.decode(msg)
        handle_partitions_view_event(version, partitions)
        return
