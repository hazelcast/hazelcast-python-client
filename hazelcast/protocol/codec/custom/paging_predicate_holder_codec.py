from hazelcast.protocol.builtin import FixSizedTypesCodec, CodecUtil
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import END_FRAME_BUF, SIZE_OF_FRAME_LENGTH_AND_FLAGS, create_initial_buffer_custom
from hazelcast.protocol import PagingPredicateHolder
from hazelcast.protocol.codec.custom.anchor_data_list_holder_codec import AnchorDataListHolderCodec
from hazelcast.protocol.builtin import DataCodec

_PAGE_SIZE_OFFSET = 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS
_PAGE_OFFSET = _PAGE_SIZE_OFFSET + INT_SIZE_IN_BYTES
_ITERATION_TYPE_ID_OFFSET = _PAGE_OFFSET + INT_SIZE_IN_BYTES
_INITIAL_FRAME_SIZE = _ITERATION_TYPE_ID_OFFSET + BYTE_SIZE_IN_BYTES - 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS


class PagingPredicateHolderCodec(object):
    @staticmethod
    def encode(buf, paging_predicate_holder):
        initial_frame_buf = create_initial_buffer_custom(_INITIAL_FRAME_SIZE, False)
        FixSizedTypesCodec.encode_int(initial_frame_buf, _PAGE_SIZE_OFFSET, paging_predicate_holder.page_size)
        FixSizedTypesCodec.encode_int(initial_frame_buf, _PAGE_OFFSET, paging_predicate_holder.page)
        FixSizedTypesCodec.encode_byte(initial_frame_buf, _ITERATION_TYPE_ID_OFFSET, paging_predicate_holder.iteration_type_id)
        buf.extend(initial_frame_buf)
        AnchorDataListHolderCodec.encode(buf, paging_predicate_holder.anchor_data_list_holder)
        CodecUtil.encode_nullable(buf, paging_predicate_holder.predicate_data, DataCodec.encode)
        CodecUtil.encode_nullable(buf, paging_predicate_holder.comparator_data, DataCodec.encode)
        CodecUtil.encode_nullable(buf, paging_predicate_holder.partition_key_data, DataCodec.encode)
        buf.extend(END_FRAME_BUF)

    @staticmethod
    def decode(msg):
        msg.next_frame()
        initial_frame = msg.next_frame()
        page_size = FixSizedTypesCodec.decode_int(initial_frame.buf, _PAGE_SIZE_OFFSET)
        page = FixSizedTypesCodec.decode_int(initial_frame.buf, _PAGE_OFFSET)
        iteration_type_id = FixSizedTypesCodec.decode_byte(initial_frame.buf, _ITERATION_TYPE_ID_OFFSET)
        anchor_data_list_holder = AnchorDataListHolderCodec.decode(msg)
        predicate_data = CodecUtil.decode_nullable(msg, DataCodec.decode)
        comparator_data = CodecUtil.decode_nullable(msg, DataCodec.decode)
        partition_key_data = CodecUtil.decode_nullable(msg, DataCodec.decode)
        CodecUtil.fast_forward_to_end_frame(msg)
        return PagingPredicateHolder(anchor_data_list_holder, predicate_data, comparator_data, page_size, page, iteration_type_id, partition_key_data)
