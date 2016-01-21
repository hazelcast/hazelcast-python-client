from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.custom_codec import *
from hazelcast.util import ImmutableLazyDataList
from hazelcast.protocol.codec.enterprise_map_message_type import *

REQUEST_TYPE = ENTERPRISEMAP_PUBLISHERCREATE
RESPONSE_TYPE = 106
RETRYABLE = True


def calculate_size(map_name, cache_name, predicate, batch_size, buffer_size, delay_seconds, populate, coalesce):
    """ Calculates the request payload size"""
    data_size = 0
    data_size += calculate_size_str(map_name)
    data_size += calculate_size_str(cache_name)
    data_size += calculate_size_data(predicate)
    data_size += INT_SIZE_IN_BYTES
    data_size += INT_SIZE_IN_BYTES
    data_size += LONG_SIZE_IN_BYTES
    data_size += BOOLEAN_SIZE_IN_BYTES
    data_size += BOOLEAN_SIZE_IN_BYTES
    return data_size


def encode_request(map_name, cache_name, predicate, batch_size, buffer_size, delay_seconds, populate, coalesce):
    """ Encode request into client_message"""
    client_message = ClientMessage(payload_size=calculate_size(map_name, cache_name, predicate, batch_size, buffer_size, delay_seconds, populate, coalesce))
    client_message.set_message_type(REQUEST_TYPE)
    client_message.set_retryable(RETRYABLE)
    client_message.append_str(map_name)
    client_message.append_str(cache_name)
    client_message.append_data(predicate)
    client_message.append_int(batch_size)
    client_message.append_int(buffer_size)
    client_message.append_long(delay_seconds)
    client_message.append_bool(populate)
    client_message.append_bool(coalesce)
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



