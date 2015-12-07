import logging
from hazelcast.serialization.data import Data

class SerializationService(object):
    logger = logging.getLogger("SerializationService")

    def __init__(self, client):
        self._client = client

    def to_data(self, val):
        return Data("")

    def from_data(self, data):
        return ""

