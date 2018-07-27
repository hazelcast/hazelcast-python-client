import hazelcast
import logging

from hazelcast.serialization.api import IdentifiedDataSerializable


class IncEntryProcessor(IdentifiedDataSerializable):
    FACTORY_ID = 66
    CLASS_ID = 1

    def read_data(self, object_data_input):
        pass

    def write_data(self, object_data_output):
        pass

    def get_factory_id(self):
        return self.FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
    logging.getLogger().setLevel(logging.INFO)

    # Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
    hz = hazelcast.HazelcastClient()
    # Get the Distributed Map from Cluster.
    map = hz.get_map("my-distributed-map")
    # Put the integer value of 0 into the Distributed Map
    map.put("key", 0)
    # Run the IncEntryProcessor class on the Hazelcast Cluster Member holding the key called "key"
    map.execute_on_key("key", IncEntryProcessor())
    # Show that the IncEntryProcessor updated the value.
    print("new value: {}".format(map.get("key").result()))
    # Shutdown this Hazelcast Client
    hz.shutdown()
