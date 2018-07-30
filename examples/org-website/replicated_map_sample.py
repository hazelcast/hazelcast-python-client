import hazelcast
import logging

if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
    logging.getLogger().setLevel(logging.INFO)

    # Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
    hz = hazelcast.HazelcastClient()
    # Get a Replicated Map called "my-replicated-map"
    rmap = hz.get_replicated_map("my-replicated-map")
    # Put and Get a value from the Replicated Map
    replacedValue = rmap.put("key", "value").result() # Will be null as its first update
    # key/value replicated to all members
    print("replacedValue = {}".format(replacedValue))
    # Will be null as its first update
    value = rmap.get("key").result()
    # the value is retrieved from a random member in the cluster
    print("value for key = {}".format(value))
    # Shutdown this Hazelcast Client
    hz.shutdown()
