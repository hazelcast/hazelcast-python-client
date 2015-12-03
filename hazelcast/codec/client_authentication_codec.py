from hazelcast.codec.util import decode_address
from hazelcast.message import ClientMessageBuilder

MESSAGE_TYPE = 0x2

def encode_request(username, password, uuid, owner_uuid, is_owner_connection, client_type, serialization_version):
    message = ClientMessageBuilder()\
        .set_message_type(MESSAGE_TYPE)\
        .set_str(username)\
        .set_str(password)

    if uuid is None:
        message.set_bool(True)
    else:
        message.set_bool(False).set_str(uuid)

    if owner_uuid is None:
        message.set_bool(True)
    else:
        message.set_bool(False).set_str(owner_uuid)

    return message.set_bool(is_owner_connection)\
        .set_str(client_type)\
        .set_byte(serialization_version)

def decode_response(message):
    resp = {}
    resp["status"] = message.read_byte()
    address_is_null = message.read_bool()
    if not address_is_null:
        resp["address"] = decode_address(message)

    uuid_is_null = message.read_bool()
    if not uuid_is_null:
        resp["uuid"] = message.read_str()

    owner_uuid_is_null = message.read_bool()
    if not owner_uuid_is_null:
        resp["owner_uuid"] = message.read_str()

    resp["serialization_version"] = message.read_byte()
    return resp
