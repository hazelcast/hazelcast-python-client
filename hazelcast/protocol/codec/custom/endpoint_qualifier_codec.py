from hazelcast.protocol.builtin import FixSizedTypesCodec, CodecUtil
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import END_FRAME_BUF, SIZE_OF_FRAME_LENGTH_AND_FLAGS, create_initial_buffer_custom
from hazelcast.protocol.builtin import StringCodec

_TYPE_OFFSET = 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS
_INITIAL_FRAME_SIZE = _TYPE_OFFSET + INT_SIZE_IN_BYTES - 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS


class EndpointQualifierCodec(object):
    @staticmethod
    def encode(buf, endpoint_qualifier):
        initial_frame_buf = create_initial_buffer_custom(_INITIAL_FRAME_SIZE, False)
        FixSizedTypesCodec.encode_int(initial_frame_buf, _TYPE_OFFSET, endpoint_qualifier.type)
        buf.extend(initial_frame_buf)
        CodecUtil.encode_nullable(buf, endpoint_qualifier.identifier, StringCodec.encode)
        buf.extend(END_FRAME_BUF)

    @staticmethod
    def decode(msg):
        msg.next_frame()
        initial_frame = msg.next_frame()
        type = FixSizedTypesCodec.decode_int(initial_frame.buf, _TYPE_OFFSET)
        identifier = CodecUtil.decode_nullable(msg, StringCodec.decode)
        CodecUtil.fast_forward_to_end_frame(msg)
        return EndpointQualifier(type, identifier)
