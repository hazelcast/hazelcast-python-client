"""
Custom Codec encoder/decoder functions used from codecs
"""
from hazelcast.core import *


class MemberCodec:
    @classmethod
    def encode(cls, client_message, member):
        AddressCodec.encode(client_message, member.address)
        client_message.append_str(member.uuid)
        client_message.append_bool(member.is_lite_member)
        client_message.append_int(len(member.attributes))
        for key, value in member.attributes:
            client_message.append_str(key)
            client_message.append_str(value)

    @classmethod
    def decode(cls, client_message):
        address = AddressCodec.decode(client_message)
        uuid = client_message.read_str()
        lite_member = client_message.read_bool()
        attribute_size = client_message.read_int()
        attributes = {}
        for i in range(0, attribute_size, 1):
            key = client_message.read_str()
            value = client_message.read_str()
            attributes[key] = value
        return Member(address, uuid, attributes, lite_member)


class AddressCodec:
    @classmethod
    def encode(cls, client_message, obj):
        client_message.append_str(obj.host).append_int(obj.port)

    @classmethod
    def decode(cls, client_message):
        host = client_message.read_str()
        port = client_message.read_int()
        return Address(host, port)


class DistributedObjectInfoCodec:
    @classmethod
    def encode(cls, client_message, obj):
        client_message.append_str(obj.service_name).append_str(obj.name)

    @classmethod
    def decode(cls, client_message):
        service_name = client_message.read_str()
        name = client_message.read_str()
        return DistributedObjectInfo(name, service_name)


class EntryViewCodec:
    @classmethod
    def encode(cls, client_message, entry_view):
        client_message.append_data(entry_view.key)
        client_message.append_data(entry_view.value)
        client_message.append_long(entry_view.cost)
        client_message.append_long(entry_view.cost)
        client_message.append_long(entry_view.creationTime)
        client_message.append_long(entry_view.expirationTime)
        client_message.append_long(entry_view.hits)
        client_message.append_long(entry_view.lastAccessTime)
        client_message.append_long(entry_view.lastStoredTime)
        client_message.append_long(entry_view.lastUpdateTime)
        client_message.append_long(entry_view.version)
        client_message.append_long(entry_view.evictionCriteriaNumber)
        client_message.append_long(entry_view.ttl)

    @classmethod
    def decode(cls, client_message):
        entry_view = EntryView()
        entry_view.key = client_message.read_data()
        entry_view.value = client_message.read_data()
        entry_view.cost = client_message.read_long()
        entry_view.cost = client_message.read_long()
        entry_view.creationTime = client_message.read_long()
        entry_view.expirationTime = client_message.read_long()
        entry_view.hits = client_message.read_long()
        entry_view.lastAccessTime = client_message.read_long()
        entry_view.lastStoredTime = client_message.read_long()
        entry_view.lastUpdateTime = client_message.read_long()
        entry_view.version = client_message.read_long()
        entry_view.evictionCriteriaNumber = client_message.read_long()
        entry_view.ttl = client_message.read_long()
        return entry_view


class QueryCacheEventDataCodec:
    @classmethod
    def encode(cls, client_message, obj):
        pass

    @classmethod
    def decode(cls, client_message):
        pass
