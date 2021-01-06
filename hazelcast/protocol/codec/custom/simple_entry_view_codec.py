from hazelcast.protocol.builtin import FixSizedTypesCodec, CodecUtil
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import END_FRAME_BUF, END_FINAL_FRAME_BUF, SIZE_OF_FRAME_LENGTH_AND_FLAGS, create_initial_buffer_custom
from hazelcast.core import SimpleEntryView
from hazelcast.protocol.builtin import DataCodec

_COST_ENCODE_OFFSET = 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS
_COST_DECODE_OFFSET = 0
_CREATION_TIME_ENCODE_OFFSET = _COST_ENCODE_OFFSET + LONG_SIZE_IN_BYTES
_CREATION_TIME_DECODE_OFFSET = _COST_DECODE_OFFSET + LONG_SIZE_IN_BYTES
_EXPIRATION_TIME_ENCODE_OFFSET = _CREATION_TIME_ENCODE_OFFSET + LONG_SIZE_IN_BYTES
_EXPIRATION_TIME_DECODE_OFFSET = _CREATION_TIME_DECODE_OFFSET + LONG_SIZE_IN_BYTES
_HITS_ENCODE_OFFSET = _EXPIRATION_TIME_ENCODE_OFFSET + LONG_SIZE_IN_BYTES
_HITS_DECODE_OFFSET = _EXPIRATION_TIME_DECODE_OFFSET + LONG_SIZE_IN_BYTES
_LAST_ACCESS_TIME_ENCODE_OFFSET = _HITS_ENCODE_OFFSET + LONG_SIZE_IN_BYTES
_LAST_ACCESS_TIME_DECODE_OFFSET = _HITS_DECODE_OFFSET + LONG_SIZE_IN_BYTES
_LAST_STORED_TIME_ENCODE_OFFSET = _LAST_ACCESS_TIME_ENCODE_OFFSET + LONG_SIZE_IN_BYTES
_LAST_STORED_TIME_DECODE_OFFSET = _LAST_ACCESS_TIME_DECODE_OFFSET + LONG_SIZE_IN_BYTES
_LAST_UPDATE_TIME_ENCODE_OFFSET = _LAST_STORED_TIME_ENCODE_OFFSET + LONG_SIZE_IN_BYTES
_LAST_UPDATE_TIME_DECODE_OFFSET = _LAST_STORED_TIME_DECODE_OFFSET + LONG_SIZE_IN_BYTES
_VERSION_ENCODE_OFFSET = _LAST_UPDATE_TIME_ENCODE_OFFSET + LONG_SIZE_IN_BYTES
_VERSION_DECODE_OFFSET = _LAST_UPDATE_TIME_DECODE_OFFSET + LONG_SIZE_IN_BYTES
_TTL_ENCODE_OFFSET = _VERSION_ENCODE_OFFSET + LONG_SIZE_IN_BYTES
_TTL_DECODE_OFFSET = _VERSION_DECODE_OFFSET + LONG_SIZE_IN_BYTES
_MAX_IDLE_ENCODE_OFFSET = _TTL_ENCODE_OFFSET + LONG_SIZE_IN_BYTES
_MAX_IDLE_DECODE_OFFSET = _TTL_DECODE_OFFSET + LONG_SIZE_IN_BYTES
_INITIAL_FRAME_SIZE = _MAX_IDLE_ENCODE_OFFSET + LONG_SIZE_IN_BYTES - 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS


class SimpleEntryViewCodec(object):
    @staticmethod
    def encode(buf, simple_entry_view, is_final=False):
        initial_frame_buf = create_initial_buffer_custom(_INITIAL_FRAME_SIZE)
        FixSizedTypesCodec.encode_long(initial_frame_buf, _COST_ENCODE_OFFSET, simple_entry_view.cost)
        FixSizedTypesCodec.encode_long(initial_frame_buf, _CREATION_TIME_ENCODE_OFFSET, simple_entry_view.creation_time)
        FixSizedTypesCodec.encode_long(initial_frame_buf, _EXPIRATION_TIME_ENCODE_OFFSET, simple_entry_view.expiration_time)
        FixSizedTypesCodec.encode_long(initial_frame_buf, _HITS_ENCODE_OFFSET, simple_entry_view.hits)
        FixSizedTypesCodec.encode_long(initial_frame_buf, _LAST_ACCESS_TIME_ENCODE_OFFSET, simple_entry_view.last_access_time)
        FixSizedTypesCodec.encode_long(initial_frame_buf, _LAST_STORED_TIME_ENCODE_OFFSET, simple_entry_view.last_stored_time)
        FixSizedTypesCodec.encode_long(initial_frame_buf, _LAST_UPDATE_TIME_ENCODE_OFFSET, simple_entry_view.last_update_time)
        FixSizedTypesCodec.encode_long(initial_frame_buf, _VERSION_ENCODE_OFFSET, simple_entry_view.version)
        FixSizedTypesCodec.encode_long(initial_frame_buf, _TTL_ENCODE_OFFSET, simple_entry_view.ttl)
        FixSizedTypesCodec.encode_long(initial_frame_buf, _MAX_IDLE_ENCODE_OFFSET, simple_entry_view.max_idle)
        buf.extend(initial_frame_buf)
        DataCodec.encode(buf, simple_entry_view.key)
        DataCodec.encode(buf, simple_entry_view.value)
        if is_final:
            buf.extend(END_FINAL_FRAME_BUF)
        else:
            buf.extend(END_FRAME_BUF)

    @staticmethod
    def decode(msg):
        msg.next_frame()
        initial_frame = msg.next_frame()
        cost = FixSizedTypesCodec.decode_long(initial_frame.buf, _COST_DECODE_OFFSET)
        creation_time = FixSizedTypesCodec.decode_long(initial_frame.buf, _CREATION_TIME_DECODE_OFFSET)
        expiration_time = FixSizedTypesCodec.decode_long(initial_frame.buf, _EXPIRATION_TIME_DECODE_OFFSET)
        hits = FixSizedTypesCodec.decode_long(initial_frame.buf, _HITS_DECODE_OFFSET)
        last_access_time = FixSizedTypesCodec.decode_long(initial_frame.buf, _LAST_ACCESS_TIME_DECODE_OFFSET)
        last_stored_time = FixSizedTypesCodec.decode_long(initial_frame.buf, _LAST_STORED_TIME_DECODE_OFFSET)
        last_update_time = FixSizedTypesCodec.decode_long(initial_frame.buf, _LAST_UPDATE_TIME_DECODE_OFFSET)
        version = FixSizedTypesCodec.decode_long(initial_frame.buf, _VERSION_DECODE_OFFSET)
        ttl = FixSizedTypesCodec.decode_long(initial_frame.buf, _TTL_DECODE_OFFSET)
        max_idle = FixSizedTypesCodec.decode_long(initial_frame.buf, _MAX_IDLE_DECODE_OFFSET)
        key = DataCodec.decode(msg)
        value = DataCodec.decode(msg)
        CodecUtil.fast_forward_to_end_frame(msg)
        return SimpleEntryView(key, value, cost, creation_time, expiration_time, hits, last_access_time, last_stored_time, last_update_time, version, ttl, max_idle)
