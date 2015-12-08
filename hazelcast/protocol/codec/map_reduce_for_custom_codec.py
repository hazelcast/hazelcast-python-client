from hazelcast.serialization.data import *
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.custom_codec import *
from hazelcast.protocol.codec.map_reduce_message_type import *

REQUEST_TYPE = MAPREDUCE_FORCUSTOM
RESPONSE_TYPE = 117
RETRYABLE = False


def calculate_size(name, job_id, predicate, mapper, combiner_factory, reducer_factory, key_value_source, chunk_size, keys, topology_changed_strategy):
    """ Calculates the request payload size"""
    data_size = 0
    data_size += calculate_size_str(name)
    data_size += calculate_size_str(job_id)
    data_size += BOOLEAN_SIZE_IN_BYTES
    if predicate is not None:
        data_size += calculate_size_data(predicate)
    data_size += calculate_size_data(mapper)
    data_size += BOOLEAN_SIZE_IN_BYTES
    if combiner_factory is not None:
        data_size += calculate_size_data(combiner_factory)
    data_size += BOOLEAN_SIZE_IN_BYTES
    if reducer_factory is not None:
        data_size += calculate_size_data(reducer_factory)
    data_size += calculate_size_data(key_value_source)
    data_size += INT_SIZE_IN_BYTES
    data_size += BOOLEAN_SIZE_IN_BYTES
    if keys is not None:
        data_size += INT_SIZE_IN_BYTES
        for keys_item in keys:
            data_size += calculate_size_data(keys_item)
    data_size += BOOLEAN_SIZE_IN_BYTES
    if topology_changed_strategy is not None:
        data_size += calculate_size_str(topology_changed_strategy)
    return data_size


def encode_request(name, job_id, predicate, mapper, combiner_factory, reducer_factory, key_value_source, chunk_size, keys, topology_changed_strategy):
    """ Encode request into client_message"""
    client_message = ClientMessage(payload_size=calculate_size(name, job_id, predicate, mapper, combiner_factory, reducer_factory, key_value_source, chunk_size, keys, topology_changed_strategy))
    client_message.set_message_type(REQUEST_TYPE)
    client_message.set_retryable(RETRYABLE)
    client_message.append_str(name)
    client_message.append_str(job_id)
    client_message.append_bool(predicate is None)
    if predicate is not None:
        client_message.append_data(predicate)
    client_message.append_data(mapper)
    client_message.append_bool(combiner_factory is None)
    if combiner_factory is not None:
        client_message.append_data(combiner_factory)
    client_message.append_bool(reducer_factory is None)
    if reducer_factory is not None:
        client_message.append_data(reducer_factory)
    client_message.append_data(key_value_source)
    client_message.append_int(chunk_size)
    client_message.append_bool(keys is None)
    if keys is not None:
        client_message.append_int(len(keys))
        for keys_item in keys:
            client_message.append_data(keys_item)
    client_message.append_bool(topology_changed_strategy is None)
    if topology_changed_strategy is not None:
        client_message.append_str(topology_changed_strategy)
    client_message.update_frame_length()
    return client_message


def decode_response(client_message):
    """ Decode response from client message"""
    parameters = dict(response=None)
    response_size = client_message.read_int()
    response = []
    for response_index in xrange(0, response_size):
        response_item = client_message.read_map_entry()
        response.append(response_item)
    parameters['response'] = response
    return parameters



