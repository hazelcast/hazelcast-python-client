from hazelcast.protocol.client_message import ClientMessage, NULL_FRAME, BEGIN_FRAME, END_FRAME, PARTITION_ID_FIELD_OFFSET, RESPONSE_BACKUP_ACKS_FIELD_OFFSET, UNFRAGMENTED_MESSAGE, TYPE_FIELD_OFFSET
import hazelcast.protocol.bits as Bits
from hazelcast.protocol.codec.builtin import *
from hazelcast.protocol.codec.custom import *
from hazelcast.core import MemberVersion

# Generated("1bb77669095053abf8b2426605acf2ff")

MAJOR_FIELD_OFFSET = 0
MINOR_FIELD_OFFSET = MAJOR_FIELD_OFFSET + Bits.BYTE_SIZE_IN_BYTES
PATCH_FIELD_OFFSET = MINOR_FIELD_OFFSET + Bits.BYTE_SIZE_IN_BYTES
INITIAL_FRAME_SIZE = PATCH_FIELD_OFFSET + Bits.BYTE_SIZE_IN_BYTES


class MemberVersionCodec(object):
    @staticmethod
    def encode(client_message, member_version):
        client_message.add(BEGIN_FRAME)

        initial_frame = ClientMessage.Frame(bytearray(INITIAL_FRAME_SIZE))
        FixedSizeTypesCodec.encode_byte(initial_frame.content, MAJOR_FIELD_OFFSET, member_version.major)
        FixedSizeTypesCodec.encode_byte(initial_frame.content, MINOR_FIELD_OFFSET, member_version.minor)
        FixedSizeTypesCodec.encode_byte(initial_frame.content, PATCH_FIELD_OFFSET, member_version.patch)
        client_message.add(initial_frame)

        client_message.add(END_FRAME)

    @staticmethod
    def decode(iterator):
        # begin frame
        iterator.next()

        initial_frame = iterator.next()
        major = FixedSizeTypesCodec.decode_byte(initial_frame.content, MAJOR_FIELD_OFFSET)
        minor = FixedSizeTypesCodec.decode_byte(initial_frame.content, MINOR_FIELD_OFFSET)
        patch = FixedSizeTypesCodec.decode_byte(initial_frame.content, PATCH_FIELD_OFFSET)

        CodecUtil.fast_forward_to_end_frame(iterator)

        return MemberVersion(major, minor, patch)