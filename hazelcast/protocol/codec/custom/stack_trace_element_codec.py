from hazelcast.protocol.client_message import ClientMessage, NULL_FRAME, BEGIN_FRAME, END_FRAME, PARTITION_ID_FIELD_OFFSET, RESPONSE_BACKUP_ACKS_FIELD_OFFSET, UNFRAGMENTED_MESSAGE, TYPE_FIELD_OFFSET
import hazelcast.protocol.bits as Bits
from hazelcast.protocol.codec.builtin import *
from hazelcast.protocol.codec.custom import *
from hazelcast.protocol.stack_trace_element import StackTraceElement

# Generated("df8b9c5e36c8b9a81e081f1ed752c461")

LINE_NUMBER_FIELD_OFFSET = 0
INITIAL_FRAME_SIZE = LINE_NUMBER_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES

class StackTraceElementCodec:
    @staticmethod
    def encode(client_message, stack_trace_element):
        client_message.add(BEGIN_FRAME)

        initial_frame = ClientMessage.Frame(bytearray(INITIAL_FRAME_SIZE))
        FixedSizeTypesCodec.encode_int(initial_frame.content, LINE_NUMBER_FIELD_OFFSET, stack_trace_element.line_number)
        client_message.add(initial_frame)

        StringCodec.encode(client_message, stack_trace_element.class_name)
        StringCodec.encode(client_message, stack_trace_element.method_name)
        CodecUtil.encode_nullable(client_message, stack_trace_element.file_name(), StringCodec.encode)

        client_message.add(END_FRAME)

    @staticmethod
    def decode(iterator):
        # begin frame
        iterator.next()

        initial_frame = iterator.next()
        line_number = FixedSizeTypesCodec.decode_int(initial_frame.content, LINE_NUMBER_FIELD_OFFSET)

        class_name = StringCodec.decode(iterator)
        method_name = StringCodec.decode(iterator)
        file_name = CodecUtil.decode_nullable(iterator, StringCodec.decode)

        CodecUtil.fast_forward_to_end_frame(iterator)

        return StackTraceElement(class_name, method_name, file_name, line_number)