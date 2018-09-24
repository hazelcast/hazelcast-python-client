from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.codec.map_message_type import *

REQUEST_TYPE = MAP_ADDINDEX
RESPONSE_TYPE = 100
RETRYABLE = False


def calculate_size(name, attribute, ordered):
    """ Calculates the request payload size"""
    data_size = 0
    data_size += calculate_size_str(name)
    data_size += calculate_size_str(attribute)
    data_size += BOOLEAN_SIZE_IN_BYTES
    return data_size


def encode_request(name, attribute, ordered):
    """ Encode request into client_message"""
    client_message = ClientMessage(payload_size=calculate_size(name, attribute, ordered))
    client_message.set_message_type(REQUEST_TYPE)
    client_message.set_retryable(RETRYABLE)
    client_message.append_str(name)
    client_message.append_str(attribute)
    client_message.append_bool(ordered)
    client_message.update_frame_length()
    return client_message


# Empty decode_response(client_message), this message has no parameters to decode
