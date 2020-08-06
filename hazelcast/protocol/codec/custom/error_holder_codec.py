from hazelcast.protocol.builtin import FixSizedTypesCodec, CodecUtil
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import END_FRAME_BUF, SIZE_OF_FRAME_LENGTH_AND_FLAGS, create_initial_buffer_custom
from hazelcast.protocol import ErrorHolder
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import ListMultiFrameCodec
from hazelcast.protocol.codec.custom import StackTraceElementCodec

_ERROR_CODE_OFFSET = 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS
_INITIAL_FRAME_SIZE = _ERROR_CODE_OFFSET + INT_SIZE_IN_BYTES - 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS


class ErrorHolderCodec(object):
    @staticmethod
    def encode(buf, error_holder):
        initial_frame_buf = create_initial_buffer_custom(_INITIAL_FRAME_SIZE, False)
        FixSizedTypesCodec.encode_int(initial_frame_buf, _ERROR_CODE_OFFSET, error_holder.error_code)
        buf.extend(initial_frame_buf)
        StringCodec.encode(buf, error_holder.class_name)
        CodecUtil.encode_nullable(buf, error_holder.message, StringCodec.encode)
        ListMultiFrameCodec.encode(buf, error_holder.stack_trace_elements, StackTraceElementCodec.encode)
        buf.extend(END_FRAME_BUF)

    @staticmethod
    def decode(msg):
        msg.next_frame()
        initial_frame = msg.next_frame()
        error_code = FixSizedTypesCodec.decode_int(initial_frame.buf, _ERROR_CODE_OFFSET)
        class_name = StringCodec.decode(msg)
        message = CodecUtil.decode_nullable(msg, StringCodec.decode)
        stack_trace_elements = ListMultiFrameCodec.decode(msg, StackTraceElementCodec.decode)
        CodecUtil.fast_forward_to_end_frame(msg)
        return ErrorHolder(error_code, class_name, message, stack_trace_elements)
