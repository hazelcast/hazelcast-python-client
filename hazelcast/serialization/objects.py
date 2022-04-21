from hazelcast.core import Address, MapEntry
from hazelcast.serialization.api import IdentifiedDataSerializable
from hazelcast.serialization.data import Data
from hazelcast.util import to_millis


# Had to put this class into the serialization module
# since it is required in two places.
# 1 - In the serialization service construction, since
#     its factory is registered.
# 2 - In ReliableTopic proxy, since publish wraps messages
#     into this type.
# If this class was at the proxy module, we would get
# cyclic dependencies.
class ReliableTopicMessage(IdentifiedDataSerializable):
    """The Object that is going to be stored in the Ringbuffer.
    It contains the actual message payload and some metadata.
    """

    FACTORY_ID = -9
    CLASS_ID = 2

    def __init__(self, publish_time=None, publisher_address=None, payload=None):
        # publish_time is in seconds but server sends/expects to receive
        # it in milliseconds.
        self.publish_time = publish_time
        self.publisher_address = publisher_address
        self.payload = payload

    def read_data(self, object_data_input):
        self.publish_time = object_data_input.read_long() / 1000.0
        self.publisher_address = object_data_input.read_object()
        self.payload = _read_data_from(object_data_input)

    def write_data(self, object_data_output):
        object_data_output.write_long(to_millis(self.publish_time))
        object_data_output.write_object(self.publisher_address)
        _write_data_to(object_data_output, self.payload)

    def get_factory_id(self):
        return self.FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID


# Inherits from the Address, as we cannot implement
# IDS in the core module due to cyclic import problems.
# This was needed to construct the ReliableTopic messages
# sent from the server as they have a non-None address
# field.
class IdentifiedAddress(Address, IdentifiedDataSerializable):

    FACTORY_ID = 0
    CLASS_ID = 1

    def __init__(self, host=None, port=None):
        super(IdentifiedAddress, self).__init__(host, port)

    def write_data(self, object_data_output):
        # skip writing it as we don't need it
        pass

    def read_data(self, object_data_input):
        self.port = object_data_input.read_int()
        # skip type as it is unused in the Python client
        object_data_input.read_byte()
        self.host = object_data_input.read_string()

    def get_factory_id(self):
        return self.FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID


# Values are canonicalized in the server-side. No need to
# do the same here.
class CanonicalizingHashSet(set, IdentifiedDataSerializable):

    FACTORY_ID = -29
    CLASS_ID = 19

    def write_data(self, object_data_output):
        pass

    def read_data(self, object_data_input):
        count = object_data_input.read_int()
        for _ in range(count):
            self.add(object_data_input.read_object())

    def get_factory_id(self):
        return self.FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID


class IdentifiedMapEntry(MapEntry, IdentifiedDataSerializable):

    FACTORY_ID = -4
    CLASS_ID = 120

    def write_data(self, object_data_output):
        pass

    def read_data(self, object_data_input):
        self._key = object_data_input.read_object()
        self._value = object_data_input.read_object()

    def get_factory_id(self):
        return self.FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID


def _read_data_from(inp):
    array = inp.read_byte_array()
    if array is None:
        return None
    return Data(array)


def _write_data_to(out, data):
    out.write_byte_array(data.buffer)
