from hazelcast.protocol.client_message import ClientMessage, NULL_FRAME, BEGIN_FRAME, END_FRAME, PARTITION_ID_FIELD_OFFSET, RESPONSE_BACKUP_ACKS_FIELD_OFFSET, UNFRAGMENTED_MESSAGE, TYPE_FIELD_OFFSET
import hazelcast.protocol.bits as Bits
from hazelcast.protocol.codec.builtin import *
from hazelcast.protocol.codec.custom import *
from hazelcast.core import SimpleEntryView

# Generated("fa604fdad237bab5942122702f1bc4e2")

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


def encode(client_message, simple_entry_view):
    client_message.add(BEGIN_FRAME)

    initial_frame = ClientMessage.Frame(bytearray(INITIAL_FRAME_SIZE))
    fixed_size_types_codec.encode_long(initial_frame.content, COST_FIELD_OFFSET, simple_entry_view.cost)
    fixed_size_types_codec.encode_long(initial_frame.content, CREATION_TIME_FIELD_OFFSET, simple_entry_view.creation_time)
    fixed_size_types_codec.encode_long(initial_frame.content, EXPIRATION_TIME_FIELD_OFFSET, simple_entry_view.expiration_time)
    fixed_size_types_codec.encode_long(initial_frame.content, HITS_FIELD_OFFSET, simple_entry_view.hits)
    fixed_size_types_codec.encode_long(initial_frame.content, LAST_ACCESS_TIME_FIELD_OFFSET, simple_entry_view.last_access_time)
    fixed_size_types_codec.encode_long(initial_frame.content, LAST_STORED_TIME_FIELD_OFFSET, simple_entry_view.last_stored_time)
    fixed_size_types_codec.encode_long(initial_frame.content, LAST_UPDATE_TIME_FIELD_OFFSET, simple_entry_view.last_update_time)
    fixed_size_types_codec.encode_long(initial_frame.content, VERSION_FIELD_OFFSET, simple_entry_view.version)
    fixed_size_types_codec.encode_long(initial_frame.content, TTL_FIELD_OFFSET, simple_entry_view.ttl)
    fixed_size_types_codec.encode_long(initial_frame.content, MAX_IDLE_FIELD_OFFSET, simple_entry_view.max_idle)
    client_message.add(initial_frame)

    data_codec.encode(client_message, simple_entry_view.key)
    data_codec.encode(client_message, simple_entry_view.value)

    client_message.add(END_FRAME)


def decode(iterator):
    # begin frame
    iterator.next()

    initial_frame = iterator.next()
    cost = fixed_size_types_codec.decode_long(initial_frame.content, COST_FIELD_OFFSET)
    creation_time = fixed_size_types_codec.decode_long(initial_frame.content, CREATION_TIME_FIELD_OFFSET)
    expiration_time = fixed_size_types_codec.decode_long(initial_frame.content, EXPIRATION_TIME_FIELD_OFFSET)
    hits = fixed_size_types_codec.decode_long(initial_frame.content, HITS_FIELD_OFFSET)
    last_access_time = fixed_size_types_codec.decode_long(initial_frame.content, LAST_ACCESS_TIME_FIELD_OFFSET)
    last_stored_time = fixed_size_types_codec.decode_long(initial_frame.content, LAST_STORED_TIME_FIELD_OFFSET)
    last_update_time = fixed_size_types_codec.decode_long(initial_frame.content, LAST_UPDATE_TIME_FIELD_OFFSET)
    version = fixed_size_types_codec.decode_long(initial_frame.content, VERSION_FIELD_OFFSET)
    ttl = fixed_size_types_codec.decode_long(initial_frame.content, TTL_FIELD_OFFSET)
    max_idle = fixed_size_types_codec.decode_long(initial_frame.content, MAX_IDLE_FIELD_OFFSET)

    key = data_codec.decode(iterator)
    value = data_codec.decode(iterator)

    codec_util.fast_forward_to_end_frame(iterator)

    return SimpleEntryView(key, value, cost, creation_time, expiration_time, hits, last_access_time, last_stored_time, last_update_time, version, ttl, max_idle)