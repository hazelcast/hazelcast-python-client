from hazelcast.protocol.codec.builtin import list_multi_frame_codec
from hazelcast.protocol.codec.builtin.list_long_codec import ListLongCodec
from hazelcast.protocol.codec.builtin import byte_array_codec
from hazelcast.protocol.client_message import BEGIN_FRAME, END_FRAME


def encode(client_message, collection):
    value_list = []
    client_message.add(BEGIN_FRAME)
    for i, value in enumerate(collection):
        value_list.append(i)
        byte_array_codec.encode(client_message, value_list)
    client_message.add(END_FRAME)
    ListLongCodec.encode(client_message, value_list)


def decode(iterator):
    list_v = list_multi_frame_codec.decode(iterator, byte_array_codec.decode)
    list_k = ListLongCodec.decode(iterator)

    result = []
    for i in range(len(list_v)):
        result.append((list_k[i], list_v[i]))

    return result
