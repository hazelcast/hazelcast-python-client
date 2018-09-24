from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.codec.list_message_type import *

REQUEST_TYPE = LIST_SET
RESPONSE_TYPE = 105
RETRYABLE = False


def calculate_size(name, index, value):
    """ Calculates the request payload size"""
    data_size = 0
    data_size += calculate_size_str(name)
    data_size += INT_SIZE_IN_BYTES
    data_size += calculate_size_data(value)
    return data_size


def encode_request(name, index, value):
    """ Encode request into client_message"""
    client_message = ClientMessage(payload_size=calculate_size(name, index, value))
    client_message.set_message_type(REQUEST_TYPE)
    client_message.set_retryable(RETRYABLE)
    client_message.append_str(name)
    client_message.append_int(index)
    client_message.append_data(value)
    client_message.update_frame_length()
    return client_message


def decode_response(client_message, to_object=None):
    """ Decode response from client message"""
    parameters = dict(response=None)
    if not client_message.read_bool():
        parameters['response'] = to_object(client_message.read_data())
    return parameters
