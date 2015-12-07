from hazelcast.serialization.data import *
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.custom_codec import *
from hazelcast.protocol.codec.client_message_type import *
from hazelcast.protocol.event_response_const import *

REQUEST_TYPE = CLIENT_ADDPARTITIONLOSTLISTENER
RESPONSE_TYPE = 104
RETRYABLE = False


def calculate_size(local_only):
    """ Calculates the request payload size"""
    data_size = 0
    data_size += BOOLEAN_SIZE_IN_BYTES
    return data_size


def encode_request(local_only):
    """ Encode request into client_message"""
    client_message = ClientMessage(payload_size=calculate_size(local_only))
    client_message.set_message_type(REQUEST_TYPE)
    client_message.set_retryable(RETRYABLE)
    client_message.append_bool(local_only)
    client_message.update_frame_length()
    return client_message


def decode_response(client_message):
    """ Decode response from client message"""
    parameters = dict(response=None)
    parameters['response'] = client_message.read_str()
    return parameters


def handle(client_message, handle_event_partitionlost = None):
    """ Event handler """
    messageType = client_message.get_message_type()
    if messageType == EVENT_PARTITIONLOST and handle_event_partitionlost is not None:
        partition_id = client_message.read_int()
        lost_backup_count = client_message.read_int()
        if not client_message.read_bool():
            source = AddressCodec.decode(client_message)
        handle_event_partitionlost(client_message, partition_id, lost_backup_count, source)

