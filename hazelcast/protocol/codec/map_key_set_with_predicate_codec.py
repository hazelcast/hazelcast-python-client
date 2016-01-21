from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.custom_codec import *
from hazelcast.util import ImmutableLazyDataList
from hazelcast.protocol.codec.map_message_type import *

REQUEST_TYPE = MAP_KEYSETWITHPREDICATE
RESPONSE_TYPE = 106
RETRYABLE = False


def calculate_size(name, predicate):
    """ Calculates the request payload size"""
    data_size = 0
    data_size += calculate_size_str(name)
    data_size += calculate_size_data(predicate)
    return data_size


def encode_request(name, predicate):
    """ Encode request into client_message"""
    client_message = ClientMessage(payload_size=calculate_size(name, predicate))
    client_message.set_message_type(REQUEST_TYPE)
    client_message.set_retryable(RETRYABLE)
    client_message.append_str(name)
    client_message.append_data(predicate)
    client_message.update_frame_length()
    return client_message


def decode_response(client_message, to_object=None):
    """ Decode response from client message"""
    parameters = dict(response=None)
    response_size = client_message.read_int()
    response = []
    for response_index in xrange(0, response_size):
        response_item = client_message.read_data()
        response.append(response_item)
    parameters['response'] = ImmutableLazyDataList(response, to_object)
    return parameters



