from hazelcast.protocol.client_message import ClientMessage, NULL_FRAME, BEGIN_FRAME, END_FRAME, PARTITION_ID_FIELD_OFFSET, RESPONSE_BACKUP_ACKS_FIELD_OFFSET, UNFRAGMENTED_MESSAGE, TYPE_FIELD_OFFSET
import hazelcast.protocol.bits as Bits
from hazelcast.protocol.codec.builtin import *
from hazelcast.protocol.codec.custom import *
from hazelcast.core import DistributedObjectInfo

# Generated("c8b1c42a96f0be9b24404cb9d0215c94")



class DistributedObjectInfoCodec(object):
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