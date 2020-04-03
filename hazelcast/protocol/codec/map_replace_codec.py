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

# Generated("19d6589e82458231fa328fd145311d97")

# hex: 0x010400
REQUEST_MESSAGE_TYPE = 66560
# hex: 0x010401
RESPONSE_MESSAGE_TYPE = 66561
REQUEST_THREAD_ID_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES
REQUEST_INITIAL_FRAME_SIZE = REQUEST_THREAD_ID_FIELD_OFFSET + Bits.LONG_SIZE_IN_BYTES
RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + Bits.BYTE_SIZE_IN_BYTES


def encode_request(name, key, value, thread_id):
    client_message = ClientMessage.create_for_encode()
    client_message.retryable = False
    client_message.operation_name = "Map.Replace"
    initial_frame = ClientMessage.Frame(bytearray(REQUEST_INITIAL_FRAME_SIZE), UNFRAGMENTED_MESSAGE)
    FixedSizeTypesCodec.encode_int(initial_frame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE)
    FixedSizeTypesCodec.encode_int(initial_frame.content, PARTITION_ID_FIELD_OFFSET, -1)
    FixedSizeTypesCodec.encode_long(initial_frame.content, REQUEST_THREAD_ID_FIELD_OFFSET, thread_id)
    client_message.add(initial_frame)
    StringCodec.encode(client_message, name)
    DataCodec.encode(client_message, key)
    DataCodec.encode(client_message, value)
    return client_message


def decode_response(client_message, to_object=None):
    iterator = client_message.frame_iterator()
    response = dict(response=None)
    #empty initial frame
    iterator.next()
    response["response"] = to_object(CodecUtil.decode_nullable(iterator, DataCodec.decode))
    return response


