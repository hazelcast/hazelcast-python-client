from hazelcast.protocol.builtin import FixSizedTypesCodec, CodecUtil
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import END_FRAME_BUF, END_FINAL_FRAME_BUF, SIZE_OF_FRAME_LENGTH_AND_FLAGS, create_initial_buffer_custom
from hazelcast.core import MemberVersion

_MAJOR_ENCODE_OFFSET = 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS
_MAJOR_DECODE_OFFSET = 0
_MINOR_ENCODE_OFFSET = _MAJOR_ENCODE_OFFSET + BYTE_SIZE_IN_BYTES
_MINOR_DECODE_OFFSET = _MAJOR_DECODE_OFFSET + BYTE_SIZE_IN_BYTES
_PATCH_ENCODE_OFFSET = _MINOR_ENCODE_OFFSET + BYTE_SIZE_IN_BYTES
_PATCH_DECODE_OFFSET = _MINOR_DECODE_OFFSET + BYTE_SIZE_IN_BYTES
_INITIAL_FRAME_SIZE = _PATCH_ENCODE_OFFSET + BYTE_SIZE_IN_BYTES - 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS


class MemberVersionCodec(object):
    @staticmethod
    def encode(buf, member_version, is_final=False):
        initial_frame_buf = create_initial_buffer_custom(_INITIAL_FRAME_SIZE)
        FixSizedTypesCodec.encode_byte(initial_frame_buf, _MAJOR_ENCODE_OFFSET, member_version.major)
        FixSizedTypesCodec.encode_byte(initial_frame_buf, _MINOR_ENCODE_OFFSET, member_version.minor)
        FixSizedTypesCodec.encode_byte(initial_frame_buf, _PATCH_ENCODE_OFFSET, member_version.patch)
        buf.extend(initial_frame_buf)
        if is_final:
            buf.extend(END_FINAL_FRAME_BUF)
        else:
            buf.extend(END_FRAME_BUF)

    @staticmethod
    def decode(msg):
        msg.next_frame()
        initial_frame = msg.next_frame()
        major = FixSizedTypesCodec.decode_byte(initial_frame.buf, _MAJOR_DECODE_OFFSET)
        minor = FixSizedTypesCodec.decode_byte(initial_frame.buf, _MINOR_DECODE_OFFSET)
        patch = FixSizedTypesCodec.decode_byte(initial_frame.buf, _PATCH_DECODE_OFFSET)
        CodecUtil.fast_forward_to_end_frame(msg)
        return MemberVersion(major, minor, patch)
