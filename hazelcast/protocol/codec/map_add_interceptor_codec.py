from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.custom_codec import *
from hazelcast.util import ImmutableLazyDataList
from hazelcast.protocol.codec.map_message_type import *

REQUEST_TYPE = MAP_ADDINTERCEPTOR
RESPONSE_TYPE = 104
RETRYABLE = False


def calculate_size(name, interceptor):
    """ Calculates the request payload size"""
    data_size = 0
    data_size += calculate_size_str(name)
    data_size += calculate_size_data(interceptor)
    return data_size


def encode_request(name, interceptor):
    """ Encode request into client_message"""
    client_message = ClientMessage(payload_size=calculate_size(name, interceptor))
    client_message.set_message_type(REQUEST_TYPE)
    client_message.set_retryable(RETRYABLE)
    client_message.append_str(name)
    client_message.append_data(interceptor)
    client_message.update_frame_length()
    return client_message


def decode_response(client_message, to_object=None):
    """ Decode response from client message"""
    parameters = dict(response=None)
    parameters['response'] = client_message.read_str()
    return parameters



