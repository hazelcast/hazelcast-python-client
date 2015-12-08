from hazelcast.serialization.data import *
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.custom_codec import *
from hazelcast.protocol.codec.enterprise_map_message_type import *

REQUEST_TYPE = ENTERPRISEMAP_SETREADCURSOR
RESPONSE_TYPE = 101
RETRYABLE = False


def calculate_size(map_name, cache_name, sequence):
    """ Calculates the request payload size"""
    data_size = 0
    data_size += calculate_size_str(map_name)
    data_size += calculate_size_str(cache_name)
    data_size += LONG_SIZE_IN_BYTES
    return data_size


def encode_request(map_name, cache_name, sequence):
    """ Encode request into client_message"""
    client_message = ClientMessage(payload_size=calculate_size(map_name, cache_name, sequence))
    client_message.set_message_type(REQUEST_TYPE)
    client_message.set_retryable(RETRYABLE)
    client_message.append_str(map_name)
    client_message.append_str(cache_name)
    client_message.append_long(sequence)
    client_message.update_frame_length()
    return client_message


def decode_response(client_message):
    """ Decode response from client message"""
    parameters = dict(response=None)
    parameters['response'] = client_message.read_bool()
    return parameters



