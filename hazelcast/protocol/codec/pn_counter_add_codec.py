from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.custom_codec import *
from hazelcast.util import ImmutableLazyDataList
from hazelcast.protocol.codec.pn_counter_message_type import *
from hazelcast.six.moves import range

REQUEST_TYPE = PNCOUNTER_ADD
RESPONSE_TYPE = 127
RETRYABLE = False


def calculate_size(name, delta, get_before_update, replica_timestamps, target_replica):
    """ Calculates the request payload size"""
    data_size = 0
    data_size += calculate_size_str(name)
    data_size += LONG_SIZE_IN_BYTES
    data_size += BOOLEAN_SIZE_IN_BYTES
    data_size += INT_SIZE_IN_BYTES
    for replica_timestamps_item in replica_timestamps:
        key = replica_timestamps_item[0]
        val = replica_timestamps_item[1]
        data_size += calculate_size_str(key)
        data_size += LONG_SIZE_IN_BYTES

    data_size += calculate_size_address(target_replica)
    return data_size


def encode_request(name, delta, get_before_update, replica_timestamps, target_replica):
    """ Encode request into client_message"""
    client_message = ClientMessage(payload_size=calculate_size(name, delta, get_before_update, replica_timestamps, target_replica))
    client_message.set_message_type(REQUEST_TYPE)
    client_message.set_retryable(RETRYABLE)
    client_message.append_str(name)
    client_message.append_long(delta)
    client_message.append_bool(get_before_update)
    client_message.append_int(len(replica_timestamps))
    for replica_timestamps_item in replica_timestamps:
        key = replica_timestamps_item[0]
        val = replica_timestamps_item[1]
        client_message.append_str(key)
        client_message.append_long(val)

    AddressCodec.encode(client_message, target_replica)
    client_message.update_frame_length()
    return client_message


def decode_response(client_message, to_object=None):
    """ Decode response from client message"""
    parameters = dict(value=None, replica_timestamps=None, replica_count=None)
    parameters['value'] = client_message.read_long()

    replica_timestamps_size = client_message.read_int()
    replica_timestamps = []
    for _ in range(0, replica_timestamps_size):
        replica_timestamps_item = (client_message.read_str(), client_message.read_long())
        replica_timestamps.append(replica_timestamps_item)

    parameters['replica_timestamps'] = ImmutableLazyDataList(replica_timestamps, to_object)
    parameters['replica_count'] = client_message.read_int()
    return parameters
