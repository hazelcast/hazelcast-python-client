from hazelcast.protocol.builtin import FixSizedTypesCodec, CodecUtil
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import END_FRAME_BUF, END_FINAL_FRAME_BUF, SIZE_OF_FRAME_LENGTH_AND_FLAGS, create_initial_buffer_custom
from hazelcast.protocol import EndpointQualifier
from hazelcast.protocol.builtin import StringCodec

_TYPE_ENCODE_OFFSET = 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS
_TYPE_DECODE_OFFSET = 0
_INITIAL_FRAME_SIZE = _TYPE_ENCODE_OFFSET + INT_SIZE_IN_BYTES - 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS


class EndpointQualifierCodec(object):
    @staticmethod
    def encode(buf, endpoint_qualifier, is_final=False):
        initial_frame_buf = create_initial_buffer_custom(_INITIAL_FRAME_SIZE)
        FixSizedTypesCodec.encode_int(initial_frame_buf, _TYPE_ENCODE_OFFSET, endpoint_qualifier.type)
        buf.extend(initial_frame_buf)
        CodecUtil.encode_nullable(buf, endpoint_qualifier.identifier, StringCodec.encode)
        if is_final:
            buf.extend(END_FINAL_FRAME_BUF)
        else:
            buf.extend(END_FRAME_BUF)

    @staticmethod
    def decode(msg):
        msg.next_frame()
        initial_frame = msg.next_frame()
        type = FixSizedTypesCodec.decode_int(initial_frame.buf, _TYPE_DECODE_OFFSET)
        identifier = CodecUtil.decode_nullable(msg, StringCodec.decode)
        CodecUtil.fast_forward_to_end_frame(msg)
        return EndpointQualifier(type, identifier)
