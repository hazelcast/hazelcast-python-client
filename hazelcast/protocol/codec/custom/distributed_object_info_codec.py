from hazelcast.protocol.client_message import ClientMessage, NULL_FRAME, BEGIN_FRAME, END_FRAME, PARTITION_ID_FIELD_OFFSET, RESPONSE_BACKUP_ACKS_FIELD_OFFSET, UNFRAGMENTED_MESSAGE, TYPE_FIELD_OFFSET
import hazelcast.protocol.bits as Bits
from hazelcast.protocol.codec.builtin import *
from hazelcast.protocol.codec.custom import *
from hazelcast.core import DistributedObjectInfo

# Generated("80bd48d2b5368c3a23fb6d8f15a90d26")


class DistributedObjectInfoCodec:
    @staticmethod
    def encode(client_message, distributed_object_info):
        client_message.add(BEGIN_FRAME)

        StringCodec.encode(client_message, distributed_object_info.service_name)
        StringCodec.encode(client_message, distributed_object_info.name)

        client_message.add(END_FRAME)

    @staticmethod
    def decode(iterator):
        # begin frame
        iterator.next()

        service_name = StringCodec.decode(iterator)
        name = StringCodec.decode(iterator)

        CodecUtil.fast_forward_to_end_frame(iterator)

        return DistributedObjectInfo(service_name, name)