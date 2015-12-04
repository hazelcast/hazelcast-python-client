from hazelcast.codec.util import decode_address
from hazelcast.message import ClientMessageBuilder

MESSAGE_TYPE = 0x8

def encode_request():
    message = ClientMessageBuilder()
    message.set_message_type(MESSAGE_TYPE)
    return message

def decode_response(message):
    resp = {}
    size = message.read_int()
    partitions = {}
    for _ in xrange(0, size):
        address = decode_address(message)
        partition_list_size = message.read_int()
        partition_list = [message.read_int() for _ in xrange(0, partition_list_size)]
        partitions[address] = partition_list
    resp["partitions"] = partitions
    return resp
