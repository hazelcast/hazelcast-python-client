from hazelcast.protocol.client_message import BEGIN_FRAME, END_FRAME
from hazelcast.protocol.codec.builtin import list_integer_codec
from hazelcast.protocol.codec.builtin import list_multi_frame_codec
from hazelcast.protocol.codec.builtin import list_uuid_codec


def encode(client_message, collection):
    key_list = []
    client_message.add(BEGIN_FRAME)
    for i, value in enumerate(collection):
        key_list.append(i)
        list_integer_codec.encode(client_message, value)

    client_message.add(END_FRAME)
    list_uuid_codec.encode(client_message, key_list)


def decode(iterator):
    list_v = list_multi_frame_codec.decode(iterator, list_integer_codec.decode)
    list_k = list_uuid_codec.decode(iterator)

    result = []
    for i in range(len(list_v)):
        result.append((list_k[i], list_v[i]))

    return result
