from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.custom_codec import *
from hazelcast.util import ImmutableLazyDataList
from hazelcast.protocol.codec.set_message_type import *

REQUEST_TYPE = SET_CONTAINSALL
RESPONSE_TYPE = 101
RETRYABLE = False


def calculate_size(name, items):
    """ Calculates the request payload size"""
    data_size = 0
    data_size += calculate_size_str(name)
    data_size += INT_SIZE_IN_BYTES
    for items_item in items:
        data_size += calculate_size_data(items_item)
    return data_size


def encode_request(name, items):
    """ Encode request into client_message"""
    client_message = ClientMessage(payload_size=calculate_size(name, items))
    client_message.set_message_type(REQUEST_TYPE)
    client_message.set_retryable(RETRYABLE)
    client_message.append_str(name)
    client_message.append_int(len(items))
    for items_item in items:
        client_message.append_data(items_item)
    client_message.update_frame_length()
    return client_message


def decode_response(client_message, to_object=None):
    """ Decode response from client message"""
    parameters = dict(response=None)
    parameters['response'] = client_message.read_bool()
    return parameters



