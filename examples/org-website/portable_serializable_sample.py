import hazelcast

from hazelcast.serialization.api import Portable


class Customer(Portable):
    FACTORY_ID = 1
    CLASS_ID = 1

    def __init__(self, id=None, name=None, last_order=None):
        self.id = id
        self.name = name
        self.last_order = last_order

    def read_portable(self, reader):
        self.id = reader.read_int("id")
        self.name = reader.read_string("name")
        self.last_order = reader.read_long("last_order")

    def write_portable(self, writer):
        writer.write_int("id", self.id)
        writer.write_string("name", self.name)
        writer.write_long("last_order", self.last_order)

    def get_factory_id(self):
        return self.FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID


# Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
client = hazelcast.HazelcastClient(
    portable_factories={Customer.FACTORY_ID: {Customer.CLASS_ID: Customer}}
)
# Customer can be used here
client.shutdown()
