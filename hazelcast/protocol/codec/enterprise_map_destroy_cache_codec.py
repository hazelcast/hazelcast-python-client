from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.custom_codec import *
from hazelcast.util import ImmutableLazyDataList
from hazelcast.protocol.codec.enterprise_map_message_type import *

REQUEST_TYPE = ENTERPRISEMAP_DESTROYCACHE
RESPONSE_TYPE = 101
RETRYABLE = False


def calculate_size(map_name, cache_name):
    """ Calculates the request payload size"""
    data_size = 0
    data_size += calculate_size_str(map_name)
    data_size += calculate_size_str(cache_name)
    return data_size


def encode_request(map_name, cache_name):
    """ Encode request into client_message"""
    client_message = ClientMessage(payload_size=calculate_size(map_name, cache_name))
    client_message.set_message_type(REQUEST_TYPE)
    client_message.set_retryable(RETRYABLE)
    client_message.append_str(map_name)
    client_message.append_str(cache_name)
    client_message.update_frame_length()
    return client_message


def decode_response(client_message, to_object=None):
    """ Decode response from client message"""
    parameters = dict(response=None)
    parameters['response'] = client_message.read_bool()
    return parameters



