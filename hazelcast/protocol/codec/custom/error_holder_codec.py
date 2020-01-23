from hazelcast.protocol.client_message import ClientMessage, NULL_FRAME, BEGIN_FRAME, END_FRAME, PARTITION_ID_FIELD_OFFSET, RESPONSE_BACKUP_ACKS_FIELD_OFFSET, UNFRAGMENTED_MESSAGE, TYPE_FIELD_OFFSET
import hazelcast.protocol.bits as Bits
from hazelcast.protocol.codec.builtin import *
from hazelcast.protocol.codec.custom import *
from hazelcast.protocol.exception.error_holder import ErrorHolder

# Generated("db3f1fc19389509509ff1c7673466b7b")

ERROR_CODE_FIELD_OFFSET = 0
INITIAL_FRAME_SIZE = ERROR_CODE_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES

class ErrorHolderCodec:
    @staticmethod
    def encode(client_message, error_holder):
        client_message.add(BEGIN_FRAME)

        initial_frame = ClientMessage.Frame(bytearray(INITIAL_FRAME_SIZE))
        FixedSizeTypesCodec.encode_int(initial_frame.content, ERROR_CODE_FIELD_OFFSET, error_holder.error_code)
        client_message.add(initial_frame)

        StringCodec.encode(client_message, error_holder.class_name)
        CodecUtil.encode_nullable(client_message, error_holder.message(), StringCodec.encode)
        ListMultiFrameCodec.encode(client_message, error_holder.stack_trace_elements, StackTraceElementCodec.encode)

        client_message.add(END_FRAME)

    @staticmethod
    def decode(iterator):
        # begin frame
        iterator.next()

        initial_frame = iterator.next()
        error_code = FixedSizeTypesCodec.decode_int(initial_frame.content, ERROR_CODE_FIELD_OFFSET)

        class_name = StringCodec.decode(iterator)
        message = CodecUtil.decode_nullable(iterator, StringCodec.decode)
        stack_trace_elements = ListMultiFrameCodec.decode(iterator, StackTraceElementCodec.decode)

        CodecUtil.fast_forward_to_end_frame(iterator)

        return ErrorHolder(error_code, class_name, message, stack_trace_elements)