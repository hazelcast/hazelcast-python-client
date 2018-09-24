from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.codec.lock_message_type import *

REQUEST_TYPE = LOCK_GETLOCKCOUNT
RESPONSE_TYPE = 102
RETRYABLE = True


def calculate_size(name):
    """ Calculates the request payload size"""
    data_size = 0
    data_size += calculate_size_str(name)
    return data_size


def encode_request(name):
    """ Encode request into client_message"""
    client_message = ClientMessage(payload_size=calculate_size(name))
    client_message.set_message_type(REQUEST_TYPE)
    client_message.set_retryable(RETRYABLE)
    client_message.append_str(name)
    client_message.update_frame_length()
    return client_message


def decode_response(client_message, to_object=None):
    """ Decode response from client message"""
    parameters = dict(response=None)
    parameters['response'] = client_message.read_int()
    return parameters
