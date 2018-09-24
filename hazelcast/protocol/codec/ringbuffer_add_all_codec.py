from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.codec.ringbuffer_message_type import *

REQUEST_TYPE = RINGBUFFER_ADDALL
RESPONSE_TYPE = 103
RETRYABLE = False


def calculate_size(name, value_list, overflow_policy):
    """ Calculates the request payload size"""
    data_size = 0
    data_size += calculate_size_str(name)
    data_size += INT_SIZE_IN_BYTES
    for value_list_item in value_list:
        data_size += calculate_size_data(value_list_item)
    data_size += INT_SIZE_IN_BYTES
    return data_size


def encode_request(name, value_list, overflow_policy):
    """ Encode request into client_message"""
    client_message = ClientMessage(payload_size=calculate_size(name, value_list, overflow_policy))
    client_message.set_message_type(REQUEST_TYPE)
    client_message.set_retryable(RETRYABLE)
    client_message.append_str(name)
    client_message.append_int(len(value_list))
    for value_list_item in value_list:
        client_message.append_data(value_list_item)
    client_message.append_int(overflow_policy)
    client_message.update_frame_length()
    return client_message


def decode_response(client_message, to_object=None):
    """ Decode response from client message"""
    parameters = dict(response=None)
    parameters['response'] = client_message.read_long()
    return parameters
