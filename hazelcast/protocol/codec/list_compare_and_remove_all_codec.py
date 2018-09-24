from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.codec.list_message_type import *

REQUEST_TYPE = LIST_COMPAREANDREMOVEALL
RESPONSE_TYPE = 101
RETRYABLE = False


def calculate_size(name, values):
    """ Calculates the request payload size"""
    data_size = 0
    data_size += calculate_size_str(name)
    data_size += INT_SIZE_IN_BYTES
    for values_item in values:
        data_size += calculate_size_data(values_item)
    return data_size


def encode_request(name, values):
    """ Encode request into client_message"""
    client_message = ClientMessage(payload_size=calculate_size(name, values))
    client_message.set_message_type(REQUEST_TYPE)
    client_message.set_retryable(RETRYABLE)
    client_message.append_str(name)
    client_message.append_int(len(values))
    for values_item in values:
        client_message.append_data(values_item)
    client_message.update_frame_length()
    return client_message


def decode_response(client_message, to_object=None):
    """ Decode response from client message"""
    parameters = dict(response=None)
    parameters['response'] = client_message.read_bool()
    return parameters
