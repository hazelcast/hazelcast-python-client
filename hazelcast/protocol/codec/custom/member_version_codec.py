from hazelcast.protocol.client_message import ClientMessage, NULL_FRAME, BEGIN_FRAME, END_FRAME, PARTITION_ID_FIELD_OFFSET, RESPONSE_BACKUP_ACKS_FIELD_OFFSET, UNFRAGMENTED_MESSAGE, TYPE_FIELD_OFFSET
import hazelcast.protocol.bits as Bits
from hazelcast.protocol.codec.builtin import *
from hazelcast.protocol.codec.custom import *
from hazelcast.core import MemberVersion

# Generated("81416d912827ffce149638c81b5d5fae")

MAJOR_FIELD_OFFSET = 0
MINOR_FIELD_OFFSET = MAJOR_FIELD_OFFSET + Bits.BYTE_SIZE_IN_BYTES
PATCH_FIELD_OFFSET = MINOR_FIELD_OFFSET + Bits.BYTE_SIZE_IN_BYTES
INITIAL_FRAME_SIZE = PATCH_FIELD_OFFSET + Bits.BYTE_SIZE_IN_BYTES


def encode(client_message, member_version):
    client_message.add(BEGIN_FRAME)

    initial_frame = ClientMessage.Frame(bytearray(INITIAL_FRAME_SIZE))
    fixed_size_types_codec.encode_byte(initial_frame.content, MAJOR_FIELD_OFFSET, member_version.major)
    fixed_size_types_codec.encode_byte(initial_frame.content, MINOR_FIELD_OFFSET, member_version.minor)
    fixed_size_types_codec.encode_byte(initial_frame.content, PATCH_FIELD_OFFSET, member_version.patch)
    client_message.add(initial_frame)

    client_message.add(END_FRAME)


def decode(iterator):
    # begin frame
    iterator.next()

    initial_frame = iterator.next()
    major = fixed_size_types_codec.decode_byte(initial_frame.content, MAJOR_FIELD_OFFSET)
    minor = fixed_size_types_codec.decode_byte(initial_frame.content, MINOR_FIELD_OFFSET)
    patch = fixed_size_types_codec.decode_byte(initial_frame.content, PATCH_FIELD_OFFSET)

    codec_util.fast_forward_to_end_frame(iterator)

    return MemberVersion(major, minor, patch)