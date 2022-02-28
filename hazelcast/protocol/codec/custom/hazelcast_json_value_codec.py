from hazelcast.protocol.builtin import CodecUtil
from hazelcast.protocol.client_message import END_FRAME_BUF, END_FINAL_FRAME_BUF, BEGIN_FRAME_BUF
from hazelcast.protocol.builtin import StringCodec
from hazelcast.core import HazelcastJsonValue


class HazelcastJsonValueCodec:
    @staticmethod
    def encode(buf, hazelcast_json_value, is_final=False):
        buf.extend(BEGIN_FRAME_BUF)
        StringCodec.encode(buf, hazelcast_json_value.value)
        if is_final:
            buf.extend(END_FINAL_FRAME_BUF)
        else:
            buf.extend(END_FRAME_BUF)

    @staticmethod
    def decode(msg):
        msg.next_frame()
        value = StringCodec.decode(msg)
        CodecUtil.fast_forward_to_end_frame(msg)
        return HazelcastJsonValue(value)
