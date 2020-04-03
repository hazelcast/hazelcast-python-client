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

# Generated("bb5820194be9f1d55dbe4c2d8f264ef6")

# hex: 0x050600
REQUEST_MESSAGE_TYPE = 329216
# hex: 0x050601
RESPONSE_MESSAGE_TYPE = 329217
REQUEST_INITIAL_FRAME_SIZE = PARTITION_ID_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES
RESPONSE_RESPONSE_FIELD_OFFSET = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + Bits.BYTE_SIZE_IN_BYTES

RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_RESPONSE_FIELD_OFFSET + Bits.BOOLEAN_SIZE_IN_BYTES


def encode_request(name, value_list):
    client_message = ClientMessage.create_for_encode()
    client_message.retryable = False
    client_message.operation_name = "List.AddAll"
    initial_frame = ClientMessage.Frame(bytearray(REQUEST_INITIAL_FRAME_SIZE), UNFRAGMENTED_MESSAGE)
    FixedSizeTypesCodec.encode_int(initial_frame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE)
    FixedSizeTypesCodec.encode_int(initial_frame.content, PARTITION_ID_FIELD_OFFSET, -1)
    client_message.add(initial_frame)
    StringCodec.encode(client_message, name)
    ListMultiFrameCodec.encode(client_message, value_list, DataCodec.encode)
    return client_message


def decode_response(client_message, to_object=None):
    iterator = client_message.frame_iterator()
    response = dict(response=None)
    initial_frame = iterator.next()
    response["response"] = FixedSizeTypesCodec.decode_boolean(initial_frame.content, RESPONSE_RESPONSE_FIELD_OFFSET)
    return response


