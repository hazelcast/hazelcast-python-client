from hazelcast.protocol.client_message import BEGIN_FRAME, NULL_FRAME, END_FRAME
from hazelcast.protocol.codec.builtin import codec_util


def encode(client_message, map, encode_key_func, encode_value_func):
    client_message.add(BEGIN_FRAME)
    for key, value in map.items():
        encode_key_func(client_message, key)
        encode_value_func(client_message, value)
    client_message.add(END_FRAME)


def encode_nullable(client_message, map, encode_key_func, encode_value_func):
    if map is None:
        client_message.add(NULL_FRAME)
    else:
        encode(client_message, map, encode_key_func, encode_value_func)


def decode(iterator, decode_key_func, decode_value_func):
    result = {}
    # begin frame
    iterator.next()
    while not codec_util.next_frame_is_data_structure_end_frame(iterator):
        result[decode_key_func(iterator)] = decode_value_func(iterator)
    # end frame
    iterator.next()
    return result


def decode_nullable(iterator, decode_key_func, decode_value_func):
    if codec_util.next_frame_is_null_end_frame(iterator):
        return None
    else:
        return decode(iterator, decode_key_func, decode_value_func)
