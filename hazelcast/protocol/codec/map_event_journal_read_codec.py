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

# Generated("32919fd8d105ba9bfbd88932bd9ecb8d")

# hex: 0x014200
REQUEST_MESSAGE_TYPE = 82432
# hex: 0x014201
RESPONSE_MESSAGE_TYPE = 82433
REQUEST_START_SEQUENCE_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES
REQUEST_MIN_SIZE_FIELD_OFFSET = REQUEST_START_SEQUENCE_FIELD_OFFSET + Bits.LONG_SIZE_IN_BYTES
REQUEST_MAX_SIZE_FIELD_OFFSET = REQUEST_MIN_SIZE_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES
REQUEST_INITIAL_FRAME_SIZE = REQUEST_MAX_SIZE_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES
RESPONSE_READ_COUNT_FIELD_OFFSET = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + Bits.BYTE_SIZE_IN_BYTES
RESPONSE_NEXT_SEQ_FIELD_OFFSET = RESPONSE_READ_COUNT_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES

RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_NEXT_SEQ_FIELD_OFFSET + Bits.LONG_SIZE_IN_BYTES


def encode_request(name, start_sequence, min_size, max_size, predicate, projection):
    client_message = ClientMessage.create_for_encode()
    client_message.retryable = True
    client_message.operation_name = "Map.EventJournalRead"
    initial_frame = ClientMessage.Frame(bytearray(REQUEST_INITIAL_FRAME_SIZE), UNFRAGMENTED_MESSAGE)
    FixedSizeTypesCodec.encode_int(initial_frame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE)
    FixedSizeTypesCodec.encode_int(initial_frame.content, PARTITION_ID_FIELD_OFFSET, -1)
    FixedSizeTypesCodec.encode_long(initial_frame.content, REQUEST_START_SEQUENCE_FIELD_OFFSET, start_sequence)
    FixedSizeTypesCodec.encode_int(initial_frame.content, REQUEST_MIN_SIZE_FIELD_OFFSET, min_size)
    FixedSizeTypesCodec.encode_int(initial_frame.content, REQUEST_MAX_SIZE_FIELD_OFFSET, max_size)
    client_message.add(initial_frame)
    StringCodec.encode(client_message, name)
    CodecUtil.encode_nullable(client_message, predicate, DataCodec.encode)
    CodecUtil.encode_nullable(client_message, projection, DataCodec.encode)
    return client_message


def decode_response(client_message, to_object=None):
    iterator = client_message.frame_iterator()
    response = dict(readCount=None, items=None, itemSeqs=None, nextSeq=None)
    initial_frame = iterator.next()
    response["readCount"] = FixedSizeTypesCodec.decode_int(initial_frame.content, RESPONSE_READ_COUNT_FIELD_OFFSET)
    response["nextSeq"] = FixedSizeTypesCodec.decode_long(initial_frame.content, RESPONSE_NEXT_SEQ_FIELD_OFFSET)
    response["items"] = ImmutableLazyDataList(ListMultiFrameCodec.decode(iterator, DataCodec.decode),to_object)
    response["itemSeqs"] = CodecUtil.decode_nullable(iterator, LongArrayCodec.decode)
    return response


