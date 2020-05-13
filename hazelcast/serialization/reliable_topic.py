from hazelcast.serialization.api import IdentifiedDataSerializable


class ReliableTopicMessage(IdentifiedDataSerializable):
    FACTORY_ID = -18
    CLASS_ID = 2

    def __init__(self, publish_time=None, publisher_address=None, payload=None):
        self.publish_time = publish_time
        self.publisher_address = publisher_address
        self.payload = payload

    def read_data(self, object_data_input):
        self.publish_time = object_data_input.read_long()
        self.publisher_address = object_data_input.read_object()
        self.payload = object_data_input.read_data()

    def write_data(self, object_data_output):
        object_data_output.write_long(self.publish_time)
        object_data_output.write_object(self.publisher_address)
        object_data_output.write_data(self.payload)

    def get_factory_id(self):
        return self.FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID