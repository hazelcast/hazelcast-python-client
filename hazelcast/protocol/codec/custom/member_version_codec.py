from hazelcast.protocol.builtin import FixSizedTypesCodec, CodecUtil
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import END_FRAME_BUF, SIZE_OF_FRAME_LENGTH_AND_FLAGS, create_initial_buffer_custom
from hazelcast.core import MemberVersion

_MAJOR_OFFSET = 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS
_MINOR_OFFSET = _MAJOR_OFFSET + BYTE_SIZE_IN_BYTES
_PATCH_OFFSET = _MINOR_OFFSET + BYTE_SIZE_IN_BYTES
_INITIAL_FRAME_SIZE = _PATCH_OFFSET + BYTE_SIZE_IN_BYTES - 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS


class MemberVersionCodec(object):
    @staticmethod
    def encode(buf, member_version):
        initial_frame_buf = create_initial_buffer_custom(_INITIAL_FRAME_SIZE, False)
        FixSizedTypesCodec.encode_byte(initial_frame_buf, _MAJOR_OFFSET, member_version.major)
        FixSizedTypesCodec.encode_byte(initial_frame_buf, _MINOR_OFFSET, member_version.minor)
        FixSizedTypesCodec.encode_byte(initial_frame_buf, _PATCH_OFFSET, member_version.patch)
        buf.extend(initial_frame_buf)
        buf.extend(END_FRAME_BUF)

    @staticmethod
    def decode(msg):
        msg.next_frame()
        initial_frame = msg.next_frame()
        major = FixSizedTypesCodec.decode_byte(initial_frame.buf, _MAJOR_OFFSET)
        minor = FixSizedTypesCodec.decode_byte(initial_frame.buf, _MINOR_OFFSET)
        patch = FixSizedTypesCodec.decode_byte(initial_frame.buf, _PATCH_OFFSET)
        CodecUtil.fast_forward_to_end_frame(msg)
        return MemberVersion(major, minor, patch)
