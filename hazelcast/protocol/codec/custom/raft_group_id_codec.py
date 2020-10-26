from hazelcast.protocol.builtin import FixSizedTypesCodec, CodecUtil
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import END_FRAME_BUF, END_FINAL_FRAME_BUF, SIZE_OF_FRAME_LENGTH_AND_FLAGS, create_initial_buffer_custom
from hazelcast.protocol import RaftGroupId
from hazelcast.protocol.builtin import StringCodec

_SEED_ENCODE_OFFSET = 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS
_SEED_DECODE_OFFSET = 0
_ID_ENCODE_OFFSET = _SEED_ENCODE_OFFSET + LONG_SIZE_IN_BYTES
_ID_DECODE_OFFSET = _SEED_DECODE_OFFSET + LONG_SIZE_IN_BYTES
_INITIAL_FRAME_SIZE = _ID_ENCODE_OFFSET + LONG_SIZE_IN_BYTES - 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS


class RaftGroupIdCodec(object):
    @staticmethod
    def encode(buf, raft_group_id, is_final=False):
        initial_frame_buf = create_initial_buffer_custom(_INITIAL_FRAME_SIZE)
        FixSizedTypesCodec.encode_long(initial_frame_buf, _SEED_ENCODE_OFFSET, raft_group_id.seed)
        FixSizedTypesCodec.encode_long(initial_frame_buf, _ID_ENCODE_OFFSET, raft_group_id.id)
        buf.extend(initial_frame_buf)
        StringCodec.encode(buf, raft_group_id.name)
        if is_final:
            buf.extend(END_FINAL_FRAME_BUF)
        else:
            buf.extend(END_FRAME_BUF)

    @staticmethod
    def decode(msg):
        msg.next_frame()
        initial_frame = msg.next_frame()
        seed = FixSizedTypesCodec.decode_long(initial_frame.buf, _SEED_DECODE_OFFSET)
        id = FixSizedTypesCodec.decode_long(initial_frame.buf, _ID_DECODE_OFFSET)
        name = StringCodec.decode(msg)
        CodecUtil.fast_forward_to_end_frame(msg)
        return RaftGroupId(name, seed, id)
