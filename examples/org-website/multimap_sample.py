import hazelcast
import logging

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(format="%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s",
                        datefmt="%H:%M%:%S,")
    logging.getLogger().setLevel(logging.INFO)

    # Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
    hz = hazelcast.HazelcastClient()
    # Get the Distributed MultiMap from Cluster.
    multi_map = hz.get_multi_map("my-distributed-multimap").blocking()
    # Put values in the map against the same key
    multi_map.put("my-key", "value1")
    multi_map.put("my-key", "value2")
    multi_map.put("my-key", "value3")
    # Print out all the values for associated with key called "my-key"
    values = multi_map.get("my-key")
    print(values)
    # remove specific key/value pair
    multi_map.remove("my-key", "value2")
    # Shutdown this Hazelcast Client
    hz.shutdown()
