from hazelcast.protocol.builtin import FixSizedTypesCodec, CodecUtil
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import END_FRAME_BUF, END_FINAL_FRAME_BUF, SIZE_OF_FRAME_LENGTH_AND_FLAGS, create_initial_buffer_custom
from hazelcast.serialization.compact import FieldDescriptor
from hazelcast.protocol.builtin import StringCodec

_KIND_ENCODE_OFFSET = 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS
_KIND_DECODE_OFFSET = 0
_INITIAL_FRAME_SIZE = _KIND_ENCODE_OFFSET + INT_SIZE_IN_BYTES - SIZE_OF_FRAME_LENGTH_AND_FLAGS


class FieldDescriptorCodec:
    @staticmethod
    def encode(buf, field_descriptor, is_final=False):
        initial_frame_buf = create_initial_buffer_custom(_INITIAL_FRAME_SIZE)
        FixSizedTypesCodec.encode_int(initial_frame_buf, _KIND_ENCODE_OFFSET, field_descriptor.kind)
        buf.extend(initial_frame_buf)
        StringCodec.encode(buf, field_descriptor.name)
        if is_final:
            buf.extend(END_FINAL_FRAME_BUF)
        else:
            buf.extend(END_FRAME_BUF)

    @staticmethod
    def decode(msg):
        msg.next_frame()
        initial_frame = msg.next_frame()
        kind = FixSizedTypesCodec.decode_int(initial_frame.buf, _KIND_DECODE_OFFSET)
        name = StringCodec.decode(msg)
        CodecUtil.fast_forward_to_end_frame(msg)
        return FieldDescriptor(name, kind)
