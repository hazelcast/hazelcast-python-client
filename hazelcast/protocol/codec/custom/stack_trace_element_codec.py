from hazelcast.protocol.builtin import FixSizedTypesCodec, CodecUtil
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import END_FRAME_BUF, END_FINAL_FRAME_BUF, SIZE_OF_FRAME_LENGTH_AND_FLAGS, create_initial_buffer_custom
from hazelcast.protocol import StackTraceElement
from hazelcast.protocol.builtin import StringCodec

_LINE_NUMBER_ENCODE_OFFSET = 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS
_LINE_NUMBER_DECODE_OFFSET = 0
_INITIAL_FRAME_SIZE = _LINE_NUMBER_ENCODE_OFFSET + INT_SIZE_IN_BYTES - 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS


class StackTraceElementCodec(object):
    @staticmethod
    def encode(buf, stack_trace_element, is_final=False):
        initial_frame_buf = create_initial_buffer_custom(_INITIAL_FRAME_SIZE)
        FixSizedTypesCodec.encode_int(initial_frame_buf, _LINE_NUMBER_ENCODE_OFFSET, stack_trace_element.line_number)
        buf.extend(initial_frame_buf)
        StringCodec.encode(buf, stack_trace_element.class_name)
        StringCodec.encode(buf, stack_trace_element.method_name)
        CodecUtil.encode_nullable(buf, stack_trace_element.file_name, StringCodec.encode)
        if is_final:
            buf.extend(END_FINAL_FRAME_BUF)
        else:
            buf.extend(END_FRAME_BUF)

    @staticmethod
    def decode(msg):
        msg.next_frame()
        initial_frame = msg.next_frame()
        line_number = FixSizedTypesCodec.decode_int(initial_frame.buf, _LINE_NUMBER_DECODE_OFFSET)
        class_name = StringCodec.decode(msg)
        method_name = StringCodec.decode(msg)
        file_name = CodecUtil.decode_nullable(msg, StringCodec.decode)
        CodecUtil.fast_forward_to_end_frame(msg)
        return StackTraceElement(class_name, method_name, file_name, line_number)
