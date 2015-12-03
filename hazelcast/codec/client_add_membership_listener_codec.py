from hazelcast.message import ClientMessageBuilder
from collections import namedtuple

MESSAGE_TYPE = 0x4

def encode_request(local_only):
    builder = ClientMessageBuilder()
    builder.set_message_type(MESSAGE_TYPE)
    builder.set_bool(local_only)
    return builder

def decode_response(parser):
    params = {}
    params["response"] = parser.read_str()
    return params
