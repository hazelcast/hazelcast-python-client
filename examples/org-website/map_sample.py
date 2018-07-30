import hazelcast
import logging

if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
    logging.getLogger().setLevel(logging.INFO)

    # Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
    hz = hazelcast.HazelcastClient()
    # Get the Distributed Map from Cluster.
    map = hz.get_map("my-distributed-map")
    # Standard Put and Get
    map.put("key", "value")
    map.get("key")
    # Concurrent Map methods, optimistic updating
    map.put_if_absent("somekey", "somevalue")
    map.replace_if_same("key", "value", "newvalue")
    # Shutdown this Hazelcast Client
    hz.shutdown()
