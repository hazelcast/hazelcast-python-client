import hazelcast
import logging

from hazelcast.serialization.api import IdentifiedDataSerializable


# Entry Processor must be implemented on the server side
class EntryProcessor(IdentifiedDataSerializable):
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
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    config = hazelcast.ClientConfig()
    client = hazelcast.HazelcastClient(config)

    my_map = client.get_map("processor-map")

    my_map.put("test_key", 0)

    # Entry Processor should be implemented on the server side
    my_map.execute_on_key("test_key", EntryProcessor())

    value = my_map.get("test_key").result()
    print(value)

    client.shutdown()
