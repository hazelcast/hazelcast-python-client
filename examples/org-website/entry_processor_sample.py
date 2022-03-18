import logging
import hazelcast

from hazelcast.serialization.api import IdentifiedDataSerializable

logging.basicConfig(level=logging.INFO)


class IncEntryProcessor(IdentifiedDataSerializable):
    FACTORY_ID = 666
    CLASS_ID = 1

    def read_data(self, object_data_input):
        pass

    def write_data(self, object_data_output):
        pass

    def get_factory_id(self):
        return self.FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID


# Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
client = hazelcast.HazelcastClient()

# Get the Distributed Map from Cluster.
distributed_map = client.get_map("distributed_map").blocking()

# Put the integer value of 0 into the Distributed Map
distributed_map.put("key", 0)

# Run the IncEntryProcessor class on the Hazelcast Cluster Member holding the key called "key"
distributed_map.execute_on_key("key", IncEntryProcessor())

# Show that the IncEntryProcessor updated the value.
print(f"New value: {distributed_map.get('key')}")

# Shutdown this Hazelcast Client
client.shutdown()
