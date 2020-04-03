from hazelcast.protocol.client_message import BEGIN_FRAME,END_FRAME
from hazelcast.protocol.codec.builtin.list_integer_codec import ListIntegerCodec
from hazelcast.protocol.codec.builtin.list_multi_frame_codec import ListMultiFrameCodec
from hazelcast.protocol.codec.builtin.list_uuid_codec import ListUUIDCodec


class EntryListUUIDListIntegerCodec:
    @staticmethod
    def encode(client_message, collection):
        key_list = []
        client_message.add(BEGIN_FRAME)
        for i, value in enumerate(collection):
            key_list.append(i)
            ListIntegerCodec.encode(client_message, value)

        client_message.add(END_FRAME)
        ListUUIDCodec.encode(client_message, key_list)

    @staticmethod
    def decode(iterator):
        list_v = ListMultiFrameCodec.decode(iterator, ListIntegerCodec.decode)
        list_k = ListUUIDCodec.decode(iterator)

        result = []
        for i in range(len(list_v)):
            result.append((list_k[i], list_v[i]))

        return result