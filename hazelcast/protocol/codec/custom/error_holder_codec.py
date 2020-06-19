from hazelcast.protocol.client_message import ClientMessage, NULL_FRAME, BEGIN_FRAME, END_FRAME, PARTITION_ID_FIELD_OFFSET, RESPONSE_BACKUP_ACKS_FIELD_OFFSET, UNFRAGMENTED_MESSAGE, TYPE_FIELD_OFFSET
import hazelcast.protocol.bits as Bits
from hazelcast.protocol.codec.builtin import *
from hazelcast.protocol.codec.custom import *
from hazelcast.protocol.exception.error_holder import ErrorHolder
from hazelcast.protocol.codec.custom import stack_trace_element_codec

# Generated("a62058dc55372029c465a20dbdaae981")

ERROR_CODE_FIELD_OFFSET = 0
INITIAL_FRAME_SIZE = ERROR_CODE_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES


def encode(client_message, error_holder):
    client_message.add(BEGIN_FRAME)

    initial_frame = ClientMessage.Frame(bytearray(INITIAL_FRAME_SIZE))
    fixed_size_types_codec.encode_int(initial_frame.content, ERROR_CODE_FIELD_OFFSET, error_holder.error_code)
    client_message.add(initial_frame)

    string_codec.encode(client_message, error_holder.class_name)
    codec_util.encode_nullable(client_message, error_holder.message, string_codec.encode)
    list_multi_frame_codec.encode(client_message, error_holder.stack_trace_elements, stack_trace_element_codec.encode)

    client_message.add(END_FRAME)


def decode(iterator):
    # begin frame
    iterator.next()

    initial_frame = iterator.next()
    error_code = fixed_size_types_codec.decode_int(initial_frame.content, ERROR_CODE_FIELD_OFFSET)

    class_name = string_codec.decode(iterator)
    message = codec_util.decode_nullable(iterator, string_codec.decode)
    stack_trace_elements = list_multi_frame_codec.decode(iterator, stack_trace_element_codec.decode)

    codec_util.fast_forward_to_end_frame(iterator)

    return ErrorHolder(error_code, class_name, message, stack_trace_elements)