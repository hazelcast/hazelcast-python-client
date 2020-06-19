from hazelcast.protocol.client_message import ClientMessage, NULL_FRAME, BEGIN_FRAME, END_FRAME, PARTITION_ID_FIELD_OFFSET, RESPONSE_BACKUP_ACKS_FIELD_OFFSET, UNFRAGMENTED_MESSAGE, TYPE_FIELD_OFFSET
import hazelcast.protocol.bits as Bits
from hazelcast.protocol.codec.builtin import *
from hazelcast.protocol.codec.custom import *
from hazelcast.core import DistributedObjectInfo

# Generated("321dd7ac7c1cfdd6065d2bd050972b3b")



def encode(client_message, distributed_object_info):
    client_message.add(BEGIN_FRAME)

    string_codec.encode(client_message, distributed_object_info.service_name)
    string_codec.encode(client_message, distributed_object_info.name)

    client_message.add(END_FRAME)


def decode(iterator):
    # begin frame
    iterator.next()

    service_name = string_codec.decode(iterator)
    name = string_codec.decode(iterator)

    codec_util.fast_forward_to_end_frame(iterator)

    return DistributedObjectInfo(service_name, name)