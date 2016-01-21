from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.custom_codec import *
from hazelcast.util import ImmutableLazyDataList
from hazelcast.protocol.codec.map_reduce_message_type import *

REQUEST_TYPE = MAPREDUCE_JOBPROCESSINFORMATION
RESPONSE_TYPE = 112
RETRYABLE = True


def calculate_size(name, job_id):
    """ Calculates the request payload size"""
    data_size = 0
    data_size += calculate_size_str(name)
    data_size += calculate_size_str(job_id)
    return data_size


def encode_request(name, job_id):
    """ Encode request into client_message"""
    client_message = ClientMessage(payload_size=calculate_size(name, job_id))
    client_message.set_message_type(REQUEST_TYPE)
    client_message.set_retryable(RETRYABLE)
    client_message.append_str(name)
    client_message.append_str(job_id)
    client_message.update_frame_length()
    return client_message


def decode_response(client_message, to_object=None):
    """ Decode response from client message"""
    parameters = dict(job_partition_states=None, process_records=None)
    job_partition_states_size = client_message.read_int()
    job_partition_states = []
    for job_partition_states_index in xrange(0, job_partition_states_size):
        job_partition_states_item = JobPartitionStateCodec.decode(client_message, to_object)
        job_partition_states.append(job_partition_states_item)
    parameters['job_partition_states'] = ImmutableLazyDataList(job_partition_states, to_object)
    parameters['process_records'] = client_message.read_int()
    return parameters



