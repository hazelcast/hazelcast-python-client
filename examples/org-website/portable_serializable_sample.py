import hazelcast
import logging

from hazelcast import ClientConfig
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
        return Customer.FACTORY_ID

    def get_class_id(self):
        return Customer.CLASS_ID

    def __repr__(self):
        return '%s %s %s' % (self.id, self.name, self.last_order)


if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
    logging.getLogger().setLevel(logging.INFO)

    config = ClientConfig()
    my_factory = {Customer.CLASS_ID: Customer}
    config.serialization_config.add_portable_factory(Customer.FACTORY_ID, my_factory)
    # Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
    hz = hazelcast.HazelcastClient(config)

    # Customer can be used here

    # Shutdown this Hazelcast Client
    hz.shutdown()
