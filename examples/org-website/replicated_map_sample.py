import hazelcast
import logging

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(format="%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s",
                        datefmt="%H:%M%:%S,")
    logging.getLogger().setLevel(logging.INFO)

    # Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
    hz = hazelcast.HazelcastClient()
    # Get a Replicated Map called "my-replicated-map"
    map = hz.get_replicated_map("my-replicated-map").blocking()
    # Put and Get a value from the Replicated Map
    replaced_value = map.put("key", "value")
    # key/value replicated to all members
    print("replaced value = {}".format(replaced_value))
    # Will be None as its first update
    value = map.get("key")
    # the value is retrieved from a random member in the cluster
    print("value for key = {}".format(value))
    # Shutdown this Hazelcast Client
    hz.shutdown()
