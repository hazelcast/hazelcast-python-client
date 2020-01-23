from hazelcast.protocol.codec.builtin.list_multi_frame_codec import ListMultiFrameCodec
from hazelcast.protocol.codec.builtin.list_long_codec import ListLongCodec
from hazelcast.protocol.codec.builtin.byte_array_codec import ByteArrayCodec
from hazelcast.protocol.client_message import BEGIN_FRAME,END_FRAME

class EntryListLongByteArrayCodec:
    @staticmethod
    def encode(client_message, collection):
        value_list = []
        client_message.add(BEGIN_FRAME)
        for i,value in enumerate(collection):
            value_list.append(i)
            ByteArrayCodec.encode(client_message, value_list)
        client_message.add(END_FRAME)
        ListLongCodec.encode(client_message, value_list)

    @staticmethod
    def decode(iterator):
        list_v = ListMultiFrameCodec.decode(iterator, ByteArrayCodec.decode)
        list_k = ListLongCodec.decode(iterator)

        result = []
        for i in range(len(list_v)):
            result.append((list_k[i], list_v[i]))

        return result
