from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.custom_codec import *
from hazelcast.util import ImmutableLazyDataList
from hazelcast.protocol.codec.client_message_type import *
from hazelcast.six.moves import range

REQUEST_TYPE = CLIENT_AUTHENTICATIONCUSTOM
RESPONSE_TYPE = 107
RETRYABLE = True


def calculate_size(credentials, uuid, owner_uuid, is_owner_connection, client_type, serialization_version, client_hazelcast_version):
    """ Calculates the request payload size"""
    data_size = 0
    data_size += calculate_size_data(credentials)
    data_size += BOOLEAN_SIZE_IN_BYTES
    if uuid is not None:
        data_size += calculate_size_str(uuid)
    data_size += BOOLEAN_SIZE_IN_BYTES
    if owner_uuid is not None:
        data_size += calculate_size_str(owner_uuid)
    data_size += BOOLEAN_SIZE_IN_BYTES
    data_size += calculate_size_str(client_type)
    data_size += BYTE_SIZE_IN_BYTES
    data_size += calculate_size_str(client_hazelcast_version)
    return data_size


def encode_request(credentials, uuid, owner_uuid, is_owner_connection, client_type, serialization_version, client_hazelcast_version):
    """ Encode request into client_message"""
    client_message = ClientMessage(payload_size=calculate_size(credentials, uuid, owner_uuid, is_owner_connection, client_type, serialization_version, client_hazelcast_version))
    client_message.set_message_type(REQUEST_TYPE)
    client_message.set_retryable(RETRYABLE)
    client_message.append_data(credentials)
    client_message.append_bool(uuid is None)
    if uuid is not None:
        client_message.append_str(uuid)
    client_message.append_bool(owner_uuid is None)
    if owner_uuid is not None:
        client_message.append_str(owner_uuid)
    client_message.append_bool(is_owner_connection)
    client_message.append_str(client_type)
    client_message.append_byte(serialization_version)
    client_message.append_str(client_hazelcast_version)
    client_message.update_frame_length()
    return client_message


def decode_response(client_message, to_object=None):
    """ Decode response from client message"""
    parameters = dict(status=None, address=None, uuid=None, owner_uuid=None, serialization_version=None, server_hazelcast_version=None, client_unregistered_members=None)
    parameters['status'] = client_message.read_byte()
    if not client_message.read_bool():
        parameters['address'] = AddressCodec.decode(client_message, to_object)
    if not client_message.read_bool():
        parameters['uuid'] = client_message.read_str()
    if not client_message.read_bool():
        parameters['owner_uuid'] = client_message.read_str()
    parameters['serialization_version'] = client_message.read_byte()
    if client_message.is_complete():
        return parameters
    parameters['server_hazelcast_version'] = client_message.read_str()
    if not client_message.read_bool():
        client_unregistered_members_size = client_message.read_int()
        client_unregistered_members = []
        for _ in range(0, client_unregistered_members_size):
            client_unregistered_members_item = MemberCodec.decode(client_message, to_object)
            client_unregistered_members.append(client_unregistered_members_item)
        parameters['client_unregistered_members'] = ImmutableLazyDataList(client_unregistered_members, to_object)
    return parameters
