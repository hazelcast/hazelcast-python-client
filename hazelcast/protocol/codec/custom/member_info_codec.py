from hazelcast.protocol.client_message import ClientMessage, NULL_FRAME, BEGIN_FRAME, END_FRAME, PARTITION_ID_FIELD_OFFSET, RESPONSE_BACKUP_ACKS_FIELD_OFFSET, UNFRAGMENTED_MESSAGE, TYPE_FIELD_OFFSET
import hazelcast.protocol.bits as Bits
from hazelcast.protocol.codec.builtin import *
from hazelcast.protocol.codec.custom import *
from hazelcast.core import MemberInfo

# Generated("7f6d64046d43192f3d0e4938ff7c7033")

UUID_FIELD_OFFSET = 0
LITE_MEMBER_FIELD_OFFSET = UUID_FIELD_OFFSET + Bits.UUID_SIZE_IN_BYTES
INITIAL_FRAME_SIZE = LITE_MEMBER_FIELD_OFFSET + Bits.BOOLEAN_SIZE_IN_BYTES


def encode(client_message, member_info):
    client_message.add(BEGIN_FRAME)

    initial_frame = ClientMessage.Frame(bytearray(INITIAL_FRAME_SIZE))
    fixed_size_types_codec.encode_uuid(initial_frame.content, UUID_FIELD_OFFSET, member_info.uuid)
    fixed_size_types_codec.encode_boolean(initial_frame.content, LITE_MEMBER_FIELD_OFFSET, member_info.lite_member())
    client_message.add(initial_frame)

    address_codec.encode(client_message, member_info.address)
    map_codec.encode(client_message, member_info.attributes(), string_codec.encode, string_codec.encode)
    member_version_codec.encode(client_message, member_info.version)
    # map_codec.encode(client_message, member_info.address_map(), endpoint_qualifier_codec.encode, address_codec.encode)

    client_message.add(END_FRAME)


def decode(iterator):
    # begin frame
    iterator.next()

    initial_frame = iterator.next()
    uuid = fixed_size_types_codec.decode_uuid(initial_frame.content, UUID_FIELD_OFFSET)
    lite_member = fixed_size_types_codec.decode_boolean(initial_frame.content, LITE_MEMBER_FIELD_OFFSET)

    address = address_codec.decode(iterator)
    attributes = map_codec.decode(iterator, string_codec.decode, string_codec.decode)
    version = member_version_codec.decode(iterator)
    # address_map = map_codec.decode(iterator, endpoint_qualifier_codec.decode, address_codec.decode)

    codec_util.fast_forward_to_end_frame(iterator)

    # return MemberInfo(address, uuid, attributes, lite_member, version, address_map)
    return MemberInfo(address, uuid, attributes, lite_member, version)