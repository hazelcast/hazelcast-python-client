from hazelcast.protocol.builtin import CodecUtil
from hazelcast.protocol.client_message import END_FRAME_BUF, END_FINAL_FRAME_BUF, BEGIN_FRAME_BUF
from hazelcast.protocol.builtin import StringCodec
from hazelcast.serialization.compact import Schema
from hazelcast.protocol.builtin import ListMultiFrameCodec
from hazelcast.protocol.codec.custom.field_descriptor_codec import FieldDescriptorCodec


class SchemaCodec:
    @staticmethod
    def encode(buf, schema, is_final=False):
        buf.extend(BEGIN_FRAME_BUF)
        StringCodec.encode(buf, schema.type_name)
        ListMultiFrameCodec.encode(buf, schema.fields, FieldDescriptorCodec.encode)
        if is_final:
            buf.extend(END_FINAL_FRAME_BUF)
        else:
            buf.extend(END_FRAME_BUF)

    @staticmethod
    def decode(msg):
        msg.next_frame()
        type_name = StringCodec.decode(msg)
        fields = ListMultiFrameCodec.decode(msg, FieldDescriptorCodec.decode)
        CodecUtil.fast_forward_to_end_frame(msg)
        return Schema(type_name, fields)
