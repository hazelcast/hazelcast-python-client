from hazelcast.protocol.client_message import BEGIN_FRAME, END_FRAME, NULL_FRAME
from hazelcast.protocol.codec.builtin.codec_util import CodecUtil


class EntryListCodec:
    @staticmethod
    def encode(client_message, collection, encode_key_func, encode_value_func):
        client_message.add(BEGIN_FRAME)
        for key, value in collection.items():
            encode_key_func(client_message, key)
            encode_value_func(client_message, value)
        client_message.add(END_FRAME)

    @staticmethod
    def encode_nullable(client_message, collection, encode_key_func, encode_value_func):
        if collection is None:
            client_message.add(NULL_FRAME)
        else:
            EntryListCodec.encode(client_message, collection, encode_key_func, encode_value_func)

    @staticmethod
    def decode(iterator, decode_key_func, decode_value_func):
        result = []
        #begin frame
        iterator.next()
        while not CodecUtil.next_frame_is_data_structure_end_frame(iterator):
            key = decode_key_func(iterator)
            value = decode_value_func(iterator)
            result.append((key, value))
        #end frame
        iterator.next()
        return result

    @staticmethod
    def decode_nullable(iterator, decode_key_func, decode_value_func):
        if CodecUtil.next_frame_is_null_end_frame(iterator):
            return None
        else:
            return EntryListCodec.decode(iterator, decode_key_func, decode_value_func)



