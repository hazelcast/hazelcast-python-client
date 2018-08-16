from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import ClientMessage
from hazelcast.util import ImmutableLazyDataList
from hazelcast.protocol.codec.ringbuffer_message_type import *
from hazelcast.six.moves import range

REQUEST_TYPE = RINGBUFFER_READMANY
RESPONSE_TYPE = 115
RETRYABLE = False


def calculate_size(name, start_sequence, min_count, max_count, filter):
    """ Calculates the request payload size"""
    data_size = 0
    data_size += calculate_size_str(name)
    data_size += LONG_SIZE_IN_BYTES
    data_size += INT_SIZE_IN_BYTES
    data_size += INT_SIZE_IN_BYTES
    data_size += BOOLEAN_SIZE_IN_BYTES
    if filter is not None:
        data_size += calculate_size_data(filter)
    return data_size


def encode_request(name, start_sequence, min_count, max_count, filter):
    """ Encode request into client_message"""
    client_message = ClientMessage(payload_size=calculate_size(name, start_sequence, min_count, max_count, filter))
    client_message.set_message_type(REQUEST_TYPE)
    client_message.set_retryable(RETRYABLE)
    client_message.append_str(name)
    client_message.append_long(start_sequence)
    client_message.append_int(min_count)
    client_message.append_int(max_count)
    client_message.append_bool(filter is None)
    if filter is not None:
        client_message.append_data(filter)
    client_message.update_frame_length()
    return client_message


def decode_response(client_message, to_object=None):
    """ Decode response from client message"""
    parameters = dict(read_count=None, items=None)
    parameters['read_count'] = client_message.read_int()
    items_size = client_message.read_int()
    items = []
    for _ in range(0, items_size):
        items_item = client_message.read_data()
        items.append(items_item)
    parameters['items'] = ImmutableLazyDataList(items, to_object)
    return parameters
