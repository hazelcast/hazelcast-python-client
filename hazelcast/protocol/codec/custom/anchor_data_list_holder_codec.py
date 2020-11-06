from hazelcast.protocol.builtin import CodecUtil
from hazelcast.protocol.client_message import END_FRAME_BUF, END_FINAL_FRAME_BUF, BEGIN_FRAME_BUF
from hazelcast.protocol.builtin import ListIntegerCodec
from hazelcast.protocol import AnchorDataListHolder
from hazelcast.protocol.builtin import EntryListCodec
from hazelcast.protocol.builtin import DataCodec


class AnchorDataListHolderCodec(object):
    @staticmethod
    def encode(buf, anchor_data_list_holder, is_final=False):
        buf.extend(BEGIN_FRAME_BUF)
        ListIntegerCodec.encode(buf, anchor_data_list_holder.anchor_page_list)
        EntryListCodec.encode(buf, anchor_data_list_holder.anchor_data_list, DataCodec.encode, DataCodec.encode)
        if is_final:
            buf.extend(END_FINAL_FRAME_BUF)
        else:
            buf.extend(END_FRAME_BUF)

    @staticmethod
    def decode(msg):
        msg.next_frame()
        anchor_page_list = ListIntegerCodec.decode(msg)
        anchor_data_list = EntryListCodec.decode(msg, DataCodec.decode, DataCodec.decode)
        CodecUtil.fast_forward_to_end_frame(msg)
        return AnchorDataListHolder(anchor_page_list, anchor_data_list)
