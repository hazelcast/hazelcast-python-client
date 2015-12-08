import binascii
import logging
import marshal

from hazelcast.serialization.data import Data

DATA_HEADER = binascii.unhexlify("0000000000000066")


class SerializationService(object):
    logger = logging.getLogger("SerializationService")

    def __init__(self, client):
        self._client = client

    def to_data(self, obj):
        buff = DATA_HEADER + marshal.dumps(obj)
        return Data(buff)

    def to_object(self, data):
        return marshal.loads(data.to_bytes()[8:])
