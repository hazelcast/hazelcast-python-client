from hazelcast.protocol.client_message import ClientMessage, PARTITION_ID_FIELD_OFFSET, RESPONSE_BACKUP_ACKS_FIELD_OFFSET, UNFRAGMENTED_MESSAGE, TYPE_FIELD_OFFSET
import hazelcast.protocol.bits as Bits
from hazelcast.protocol.codec.builtin import *
from hazelcast.protocol.codec.custom import *
from hazelcast.util import ImmutableLazyDataList

"""
 * This file is auto-generated by the Hazelcast Client Protocol Code Generator.
 * To change this file, edit the templates or the protocol
 * definitions on the https://github.com/hazelcast/hazelcast-client-protocol
 * and regenerate it.
"""

# Generated("c5b629b8fc1d463de3a021dee632dc07")

# hex: 0x011F00
REQUEST_MESSAGE_TYPE = 73472
# hex: 0x011F01
RESPONSE_MESSAGE_TYPE = 73473
REQUEST_INITIAL_FRAME_SIZE = PARTITION_ID_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES
RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + Bits.BYTE_SIZE_IN_BYTES


def encode_request(name):
    client_message = ClientMessage.create_for_encode()
    client_message.retryable = False
    client_message.operation_name = "Map.EvictAll"
    initial_frame = ClientMessage.Frame(bytearray(REQUEST_INITIAL_FRAME_SIZE), UNFRAGMENTED_MESSAGE)
    FixedSizeTypesCodec.encode_int(initial_frame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE)
    FixedSizeTypesCodec.encode_int(initial_frame.content, PARTITION_ID_FIELD_OFFSET, -1)
    client_message.add(initial_frame)
    StringCodec.encode(client_message, name)
    return client_message


def decode_response(client_message, to_object=None):
    iterator = client_message.frame_iterator()
    response = dict()
    #empty initial frame
    iterator.next()
    return response


