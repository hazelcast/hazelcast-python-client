from hazelcast.protocol.client_message import BEGIN_FRAME, END_FRAME, NULL_FRAME
from hazelcast.protocol.codec.builtin import codec_util


def encode(client_message, collection, encode_function):
    client_message.add(BEGIN_FRAME.copy())

    for item in collection:
        encode_function(client_message, item)

    client_message.add(END_FRAME.copy())


def encode_contains_nullable(client_message, collection, encode_function):
    client_message.add(BEGIN_FRAME.copy())
    for item in collection:
        if item is None:
            client_message.add(NULL_FRAME.copy())
        else:
            encode_function(client_message, item)

    client_message.add(END_FRAME.copy())


def encode_nullable(client_message, collection, encode_function):
    if collection is None:
        client_message.add(NULL_FRAME.copy())
    else:
        encode(client_message, collection, encode_function)


def decode(iterator, decode_function):
    result = []

    iterator.next()
    while not codec_util.next_frame_is_data_structure_end_frame(iterator):
        result.append(decode_function(iterator))

    iterator.next()
    return result
