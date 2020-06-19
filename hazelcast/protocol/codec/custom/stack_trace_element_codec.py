from hazelcast.protocol.client_message import ClientMessage, NULL_FRAME, BEGIN_FRAME, END_FRAME, PARTITION_ID_FIELD_OFFSET, RESPONSE_BACKUP_ACKS_FIELD_OFFSET, UNFRAGMENTED_MESSAGE, TYPE_FIELD_OFFSET
import hazelcast.protocol.bits as Bits
from hazelcast.protocol.codec.builtin import *
from hazelcast.protocol.codec.custom import *
from hazelcast.protocol.stack_trace_element import StackTraceElement

# Generated("1ca4e0aeac0877057ba53b2356a10ee0")

LINE_NUMBER_FIELD_OFFSET = 0
INITIAL_FRAME_SIZE = LINE_NUMBER_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES


def encode(client_message, stack_trace_element):
    client_message.add(BEGIN_FRAME)

    initial_frame = ClientMessage.Frame(bytearray(INITIAL_FRAME_SIZE))
    fixed_size_types_codec.encode_int(initial_frame.content, LINE_NUMBER_FIELD_OFFSET, stack_trace_element.line_number)
    client_message.add(initial_frame)

    string_codec.encode(client_message, stack_trace_element.class_name)
    string_codec.encode(client_message, stack_trace_element.method_name)
    codec_util.encode_nullable(client_message, stack_trace_element.file_name, string_codec.encode)

    client_message.add(END_FRAME)


def decode(iterator):
    # begin frame
    iterator.next()

    initial_frame = iterator.next()
    line_number = fixed_size_types_codec.decode_int(initial_frame.content, LINE_NUMBER_FIELD_OFFSET)

    class_name = string_codec.decode(iterator)
    method_name = string_codec.decode(iterator)
    file_name = codec_util.decode_nullable(iterator, string_codec.decode)

    codec_util.fast_forward_to_end_frame(iterator)

    return StackTraceElement(class_name, method_name, file_name, line_number)