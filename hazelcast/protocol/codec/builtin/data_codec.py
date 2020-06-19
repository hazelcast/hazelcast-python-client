from hazelcast.protocol.client_message import ClientMessage, NULL_FRAME
from hazelcast.protocol.codec.builtin import codec_util
from hazelcast.serialization.data import Data


def encode(client_message, data):
    client_message.add(ClientMessage.Frame(bytearray(data.to_bytes())))


def encode_nullable(client_message, data):
    if data is None:
        client_message.add(NULL_FRAME)
    else:
        client_message.add(ClientMessage.Frame(bytearray(data.to_bytes())))


def decode(iterator):
    return Data(iterator.next().content)


def decode_nullable(iterator):
    if codec_util.next_frame_is_null_end_frame(iterator):
        return None
    else:
        return decode(iterator.next())
