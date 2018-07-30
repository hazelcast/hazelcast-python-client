import hazelcast
import logging

from hazelcast import ClientConfig
from hazelcast.serialization.api import IdentifiedDataSerializable


class Employee(IdentifiedDataSerializable):
    FACTORY_ID = 1000
    CLASS_ID = 100

    def __init__(self, id=None, name=None):
        self.id = id
        self.name = name

    def read_data(self, object_data_input):
        self.id = object_data_input.read_int()
        self.name = object_data_input.read_utf()

    def write_data(self, object_data_output):
        object_data_output.write_int(self.id)
        object_data_output.write_utf(self.name)

    def get_factory_id(self):
        return Employee.FACTORY_ID

    def get_class_id(self):
        return Employee.CLASS_ID

    def __repr__(self):
        return '%s %s' % (self.id, self.name)


if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
    logging.getLogger().setLevel(logging.INFO)

    config = ClientConfig()
    my_factory = {Employee.CLASS_ID: Employee}
    config.serialization_config.add_data_serializable_factory(Employee.FACTORY_ID, my_factory)

    # Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
    hz = hazelcast.HazelcastClient(config)

    # Employee can be used here

    # Shutdown this Hazelcast Client
    hz.shutdown()
