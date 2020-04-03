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

# Generated("4560ef78b7b60994a9ce2a0d9873f72c")

# hex: 0x011600
REQUEST_MESSAGE_TYPE = 71168
# hex: 0x011601
RESPONSE_MESSAGE_TYPE = 71169
REQUEST_INCLUDE_VALUE_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES
REQUEST_LISTENER_FLAGS_FIELD_OFFSET = REQUEST_INCLUDE_VALUE_FIELD_OFFSET + Bits.BOOLEAN_SIZE_IN_BYTES
REQUEST_LOCAL_ONLY_FIELD_OFFSET = REQUEST_LISTENER_FLAGS_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES
REQUEST_INITIAL_FRAME_SIZE = REQUEST_LOCAL_ONLY_FIELD_OFFSET + Bits.BOOLEAN_SIZE_IN_BYTES
RESPONSE_RESPONSE_FIELD_OFFSET = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + Bits.BYTE_SIZE_IN_BYTES

RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_RESPONSE_FIELD_OFFSET + Bits.UUID_SIZE_IN_BYTES
EVENT_ENTRY_EVENT_TYPE_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES
EVENT_ENTRY_UUID_FIELD_OFFSET = EVENT_ENTRY_EVENT_TYPE_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES
EVENT_ENTRY_NUMBER_OF_AFFECTED_ENTRIES_FIELD_OFFSET = EVENT_ENTRY_UUID_FIELD_OFFSET + Bits.UUID_SIZE_IN_BYTES
EVENT_ENTRY_INITIAL_FRAME_SIZE = EVENT_ENTRY_NUMBER_OF_AFFECTED_ENTRIES_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES
    #hex: 0x011602
EVENT_ENTRY_MESSAGE_TYPE = 71170


def encode_request(name, key, predicate, include_value, listener_flags, local_only):
    client_message = ClientMessage.create_for_encode()
    client_message.retryable = False
    client_message.operation_name = "Map.AddEntryListenerToKeyWithPredicate"
    initial_frame = ClientMessage.Frame(bytearray(REQUEST_INITIAL_FRAME_SIZE), UNFRAGMENTED_MESSAGE)
    FixedSizeTypesCodec.encode_int(initial_frame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE)
    FixedSizeTypesCodec.encode_int(initial_frame.content, PARTITION_ID_FIELD_OFFSET, -1)
    FixedSizeTypesCodec.encode_boolean(initial_frame.content, REQUEST_INCLUDE_VALUE_FIELD_OFFSET, include_value)
    FixedSizeTypesCodec.encode_int(initial_frame.content, REQUEST_LISTENER_FLAGS_FIELD_OFFSET, listener_flags)
    FixedSizeTypesCodec.encode_boolean(initial_frame.content, REQUEST_LOCAL_ONLY_FIELD_OFFSET, local_only)
    client_message.add(initial_frame)
    StringCodec.encode(client_message, name)
    DataCodec.encode(client_message, key)
    DataCodec.encode(client_message, predicate)
    return client_message


def decode_response(client_message, to_object=None):
    iterator = client_message.frame_iterator()
    response = dict(response=None)
    initial_frame = iterator.next()
    response["response"] = FixedSizeTypesCodec.decode_uuid(initial_frame.content, RESPONSE_RESPONSE_FIELD_OFFSET)
    return response


def handle(client_message, handle_entry_event=None):
    message_type = client_message.get_message_type()
    iterator = client_message.frame_iterator()
    if message_type == EVENT_ENTRY_MESSAGE_TYPE and handle_entry_event is not None:
        initial_frame = iterator.next()
        event_type = FixedSizeTypesCodec.decode_int(initial_frame.content, EVENT_ENTRY_EVENT_TYPE_FIELD_OFFSET)
        uuid = FixedSizeTypesCodec.decode_uuid(initial_frame.content, EVENT_ENTRY_UUID_FIELD_OFFSET)
        number_of_affected_entries = FixedSizeTypesCodec.decode_int(initial_frame.content, EVENT_ENTRY_NUMBER_OF_AFFECTED_ENTRIES_FIELD_OFFSET)
        key = to_object(CodecUtil.decode_nullable(iterator, DataCodec.decode))
        value = to_object(CodecUtil.decode_nullable(iterator, DataCodec.decode))
        old_value = to_object(CodecUtil.decode_nullable(iterator, DataCodec.decode))
        merging_value = to_object(CodecUtil.decode_nullable(iterator, DataCodec.decode))
        handle_entry_event(key, value, old_value, merging_value, event_type, uuid, number_of_affected_entries)
