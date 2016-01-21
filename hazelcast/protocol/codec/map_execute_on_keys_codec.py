from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.custom_codec import *
from hazelcast.util import ImmutableLazyDataList
from hazelcast.protocol.codec.map_message_type import *

REQUEST_TYPE = MAP_EXECUTEONKEYS
RESPONSE_TYPE = 117
RETRYABLE = False


def calculate_size(name, entry_processor, keys):
    """ Calculates the request payload size"""
    data_size = 0
    data_size += calculate_size_str(name)
    data_size += calculate_size_data(entry_processor)
    data_size += INT_SIZE_IN_BYTES
    for keys_item in keys:
        data_size += calculate_size_data(keys_item)
    return data_size


def encode_request(name, entry_processor, keys):
    """ Encode request into client_message"""
    client_message = ClientMessage(payload_size=calculate_size(name, entry_processor, keys))
    client_message.set_message_type(REQUEST_TYPE)
    client_message.set_retryable(RETRYABLE)
    client_message.append_str(name)
    client_message.append_data(entry_processor)
    client_message.append_int(len(keys))
    for keys_item in keys:
        client_message.append_data(keys_item)
    client_message.update_frame_length()
    return client_message


def decode_response(client_message, to_object=None):
    """ Decode response from client message"""
    parameters = dict(response=None)
    response_size = client_message.read_int()
    response = []
    for response_index in xrange(0, response_size):
        response_item = (client_message.read_data(), client_message.read_data())
        response.append(response_item)
    parameters['response'] = ImmutableLazyDataList(response, to_object)
    return parameters



