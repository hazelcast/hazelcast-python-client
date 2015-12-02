from hazelcast.message import ClientMessageBuilder

MESSAGE_TYPE = 0x2

def encode_request(username, password, uuid, owner_uuid, is_owner_connection, client_type, serialization_version):
    builder = ClientMessageBuilder()\
        .set_message_type(MESSAGE_TYPE)\
        .set_str(username)\
        .set_str(password)

    if uuid is None:
        builder.set_bool(True)
    else:
        builder.set_bool(False).set_str(uuid)

    if owner_uuid is None:
        builder.set_bool(True)
    else:
        builder.set_bool(False).set_str(owner_uuid)

    return builder.set_bool(is_owner_connection)\
        .set_str(client_type)\
        .set_byte(serialization_version)

def decode_response(parser):
    resp = {}
    resp["status"] = parser.read_byte()
    address_is_null = parser.read_bool()
    if not address_is_null:
        resp["address"] = (parser.read_str(), parser.read_int())

    uuid_is_null = parser.read_bool()
    if not uuid_is_null:
        resp["uuid"] = parser.read_str()

    owner_uuid_is_null = parser.read_bool()
    if not owner_uuid_is_null:
        resp["owner_uuid"] = parser.read_str()

    resp["serialization_version"] = parser.read_byte()
    return resp
