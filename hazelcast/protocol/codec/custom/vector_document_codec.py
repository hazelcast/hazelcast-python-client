from hazelcast.protocol.builtin import CodecUtil
from hazelcast.protocol.client_message import END_FRAME_BUF, END_FINAL_FRAME_BUF, BEGIN_FRAME_BUF
from hazelcast.protocol.builtin import DataCodec
from hazelcast.vector import VectorDocument
from hazelcast.protocol.builtin import ListMultiFrameCodec
from hazelcast.protocol.codec.custom.vector_pair_codec import VectorPairCodec


class VectorDocumentCodec:
    @staticmethod
    def encode(buf, vector_document, is_final=False):
        buf.extend(BEGIN_FRAME_BUF)
        DataCodec.encode(buf, vector_document.value)
        ListMultiFrameCodec.encode(buf, vector_document.vectors, VectorPairCodec.encode)
        if is_final:
            buf.extend(END_FINAL_FRAME_BUF)
        else:
            buf.extend(END_FRAME_BUF)

    @staticmethod
    def decode(msg):
        msg.next_frame()
        value = DataCodec.decode(msg)
        vectors = ListMultiFrameCodec.decode(msg, VectorPairCodec.decode)
        CodecUtil.fast_forward_to_end_frame(msg)
        return VectorDocument(value, vectors)
