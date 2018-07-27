import hazelcast
import logging

if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
    logging.getLogger().setLevel(logging.INFO)

    # Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
    hz = hazelcast.HazelcastClient()
    # Get the Distributed MultiMap from Cluster.
    multiMap = hz.get_multi_map("my-distributed-multimap")
    # Put values in the map against the same key
    multiMap.put("my-key", "value1")
    multiMap.put("my-key", "value2")
    multiMap.put("my-key", "value3")
    # Print out all the values for associated with key called "my-key"
    values = multiMap.get("my-key").result()
    print(values)
    # remove specific key/value pair
    multiMap.remove("my-key", "value2")
    # Shutdown this Hazelcast Client
    hz.shutdown()
