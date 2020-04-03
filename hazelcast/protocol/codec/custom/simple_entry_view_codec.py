from hazelcast.protocol.client_message import ClientMessage, NULL_FRAME, BEGIN_FRAME, END_FRAME, PARTITION_ID_FIELD_OFFSET, RESPONSE_BACKUP_ACKS_FIELD_OFFSET, UNFRAGMENTED_MESSAGE, TYPE_FIELD_OFFSET
import hazelcast.protocol.bits as Bits
from hazelcast.protocol.codec.builtin import *
from hazelcast.protocol.codec.custom import *
from hazelcast.core import EntryView

# Generated("f3c745a1584d63e0e9df66e92cc15bb8")

COST_FIELD_OFFSET = 0
CREATION_TIME_FIELD_OFFSET = COST_FIELD_OFFSET + Bits.LONG_SIZE_IN_BYTES
EXPIRATION_TIME_FIELD_OFFSET = CREATION_TIME_FIELD_OFFSET + Bits.LONG_SIZE_IN_BYTES
HITS_FIELD_OFFSET = EXPIRATION_TIME_FIELD_OFFSET + Bits.LONG_SIZE_IN_BYTES
LAST_ACCESS_TIME_FIELD_OFFSET = HITS_FIELD_OFFSET + Bits.LONG_SIZE_IN_BYTES
LAST_STORED_TIME_FIELD_OFFSET = LAST_ACCESS_TIME_FIELD_OFFSET + Bits.LONG_SIZE_IN_BYTES
LAST_UPDATE_TIME_FIELD_OFFSET = LAST_STORED_TIME_FIELD_OFFSET + Bits.LONG_SIZE_IN_BYTES
VERSION_FIELD_OFFSET = LAST_UPDATE_TIME_FIELD_OFFSET + Bits.LONG_SIZE_IN_BYTES
TTL_FIELD_OFFSET = VERSION_FIELD_OFFSET + Bits.LONG_SIZE_IN_BYTES
MAX_IDLE_FIELD_OFFSET = TTL_FIELD_OFFSET + Bits.LONG_SIZE_IN_BYTES
INITIAL_FRAME_SIZE = MAX_IDLE_FIELD_OFFSET + Bits.LONG_SIZE_IN_BYTES


class SimpleEntryViewCodec(object):
    @staticmethod
    def encode(client_message, simple_entry_view):
        client_message.add(BEGIN_FRAME)

        initial_frame = ClientMessage.Frame(bytearray(INITIAL_FRAME_SIZE))
        FixedSizeTypesCodec.encode_long(initial_frame.content, COST_FIELD_OFFSET, simple_entry_view.cost)
        FixedSizeTypesCodec.encode_long(initial_frame.content, CREATION_TIME_FIELD_OFFSET, simple_entry_view.creation_time)
        FixedSizeTypesCodec.encode_long(initial_frame.content, EXPIRATION_TIME_FIELD_OFFSET, simple_entry_view.expiration_time)
        FixedSizeTypesCodec.encode_long(initial_frame.content, HITS_FIELD_OFFSET, simple_entry_view.hits)
        FixedSizeTypesCodec.encode_long(initial_frame.content, LAST_ACCESS_TIME_FIELD_OFFSET, simple_entry_view.last_access_time)
        FixedSizeTypesCodec.encode_long(initial_frame.content, LAST_STORED_TIME_FIELD_OFFSET, simple_entry_view.last_stored_time)
        FixedSizeTypesCodec.encode_long(initial_frame.content, LAST_UPDATE_TIME_FIELD_OFFSET, simple_entry_view.last_update_time)
        FixedSizeTypesCodec.encode_long(initial_frame.content, VERSION_FIELD_OFFSET, simple_entry_view.version)
        FixedSizeTypesCodec.encode_long(initial_frame.content, TTL_FIELD_OFFSET, simple_entry_view.ttl)
        FixedSizeTypesCodec.encode_long(initial_frame.content, MAX_IDLE_FIELD_OFFSET, simple_entry_view.max_idle)
        client_message.add(initial_frame)

        DataCodec.encode(client_message, simple_entry_view.key)
        DataCodec.encode(client_message, simple_entry_view.value)

        client_message.add(END_FRAME)

    @staticmethod
    def decode(iterator):
        # begin frame
        iterator.next()

        initial_frame = iterator.next()
        cost = FixedSizeTypesCodec.decode_long(initial_frame.content, COST_FIELD_OFFSET)
        creation_time = FixedSizeTypesCodec.decode_long(initial_frame.content, CREATION_TIME_FIELD_OFFSET)
        expiration_time = FixedSizeTypesCodec.decode_long(initial_frame.content, EXPIRATION_TIME_FIELD_OFFSET)
        hits = FixedSizeTypesCodec.decode_long(initial_frame.content, HITS_FIELD_OFFSET)
        last_access_time = FixedSizeTypesCodec.decode_long(initial_frame.content, LAST_ACCESS_TIME_FIELD_OFFSET)
        last_stored_time = FixedSizeTypesCodec.decode_long(initial_frame.content, LAST_STORED_TIME_FIELD_OFFSET)
        last_update_time = FixedSizeTypesCodec.decode_long(initial_frame.content, LAST_UPDATE_TIME_FIELD_OFFSET)
        version = FixedSizeTypesCodec.decode_long(initial_frame.content, VERSION_FIELD_OFFSET)
        ttl = FixedSizeTypesCodec.decode_long(initial_frame.content, TTL_FIELD_OFFSET)
        max_idle = FixedSizeTypesCodec.decode_long(initial_frame.content, MAX_IDLE_FIELD_OFFSET)

        key = DataCodec.decode(iterator)
        value = DataCodec.decode(iterator)

        CodecUtil.fast_forward_to_end_frame(iterator)

        return EntryView(key, value, cost, creation_time, expiration_time, hits, last_access_time, last_stored_time, last_update_time, version, ttl, max_idle)