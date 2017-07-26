from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.custom_codec import *
from hazelcast.util import ImmutableLazyDataList
from hazelcast.protocol.codec.map_message_type import *
from hazelcast.protocol.event_response_const import *

REQUEST_TYPE = MAP_ADDNEARCACHEENTRYLISTENER
RESPONSE_TYPE = 104
RETRYABLE = False


def calculate_size(name, listener_flags, local_only):
    """ Calculates the request payload size"""
    data_size = 0
    data_size += calculate_size_str(name)
    data_size += INT_SIZE_IN_BYTES
    data_size += BOOLEAN_SIZE_IN_BYTES
    return data_size


def encode_request(name, listener_flags, local_only):
    """ Encode request into client_message"""
    client_message = ClientMessage(payload_size=calculate_size(name, listener_flags, local_only))
    client_message.set_message_type(REQUEST_TYPE)
    client_message.set_retryable(RETRYABLE)
    client_message.append_str(name)
    client_message.append_int(listener_flags)
    client_message.append_bool(local_only)
    client_message.update_frame_length()
    return client_message


def decode_response(client_message, to_object=None):
    """ Decode response from client message"""
    parameters = dict(response=None)
    parameters['response'] = client_message.read_str()
    return parameters


def handle(client_message, handle_event_imapinvalidation = None, handle_event_imapbatchinvalidation = None, to_object=None):
    """ Event handler """
    message_type = client_message.get_message_type()
    if message_type == EVENT_IMAPINVALIDATION and handle_event_imapinvalidation is not None:
        key=None
        if not client_message.read_bool():
            key = client_message.read_data()
        source_uuid = client_message.read_str()
        partition_uuid = UUIDCodec.decode(client_message, to_object)
        sequence = client_message.read_long()
        handle_event_imapinvalidation(key=key, source_uuid=source_uuid, partition_uuid=partition_uuid, sequence=sequence)
    if message_type == EVENT_IMAPBATCHINVALIDATION and handle_event_imapbatchinvalidation is not None:

        keys_size = client_message.read_int()
        keys = []
        for keys_index in xrange(0, keys_size):
            keys_item = client_message.read_data()
            keys.append(keys_item)

        source_uuids_size = client_message.read_int()
        source_uuids = []
        for source_uuids_index in xrange(0, source_uuids_size):
            source_uuids_item = client_message.read_str()
            source_uuids.append(source_uuids_item)

        partition_uuids_size = client_message.read_int()
        partition_uuids = []
        for partition_uuids_index in xrange(0, partition_uuids_size):
            partition_uuids_item = UUIDCodec.decode(client_message, to_object)
            partition_uuids.append(partition_uuids_item)

        sequences_size = client_message.read_int()
        sequences = []
        for sequences_index in xrange(0, sequences_size):
            sequences_item = client_message.read_java.lang.Long()
            sequences.append(sequences_item)
        handle_event_imapbatchinvalidation(keys=keys, source_uuids=source_uuids, partition_uuids=partition_uuids, sequences=sequences)

