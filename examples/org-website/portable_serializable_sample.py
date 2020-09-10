import hazelcast

from hazelcast.serialization.api import Portable


class Customer(Portable):
    FACTORY_ID = 1
    CLASS_ID = 1

    def __init__(self, id=None, name=None, last_order=None):
        self.id = id
        self.name = name
        self.last_order = last_order

    def read_portable(self, object_data_input):
        self.id = object_data_input.read_int("id")
        self.name = object_data_input.read_utf("name")
        self.last_order = object_data_input.read_long("last_order")

    def write_portable(self, object_data_output):
        object_data_output.write_int("id", self.id)
        object_data_output.write_utf("name", self.name)
        object_data_output.write_long("last_order", self.last_order)

    def get_factory_id(self):
        return self.FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID


# Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
hz = hazelcast.HazelcastClient(portable_factories={
    Customer.FACTORY_ID: {
        Customer.CLASS_ID: Customer
    }
})
# Customer can be used here
hz.shutdown()
