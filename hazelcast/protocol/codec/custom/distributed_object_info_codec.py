from hazelcast.protocol.builtin import CodecUtil
from hazelcast.protocol.client_message import END_FRAME_BUF, END_FINAL_FRAME_BUF, BEGIN_FRAME_BUF
from hazelcast.protocol.builtin import StringCodec
from hazelcast.core import DistributedObjectInfo


class DistributedObjectInfoCodec(object):
    @staticmethod
    def encode(buf, distributed_object_info, is_final=False):
        buf.extend(BEGIN_FRAME_BUF)
        StringCodec.encode(buf, distributed_object_info.service_name)
        StringCodec.encode(buf, distributed_object_info.name)
        if is_final:
            buf.extend(END_FINAL_FRAME_BUF)
        else:
            buf.extend(END_FRAME_BUF)

    @staticmethod
    def decode(msg):
        msg.next_frame()
        service_name = StringCodec.decode(msg)
        name = StringCodec.decode(msg)
        CodecUtil.fast_forward_to_end_frame(msg)
        return DistributedObjectInfo(service_name, name)
