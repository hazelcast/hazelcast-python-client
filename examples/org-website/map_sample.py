import logging
import hazelcast

logging.basicConfig(level=logging.INFO)

# Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
client = hazelcast.HazelcastClient()

# Get the Distributed Map from Cluster.
distributed_map = client.get_map("distributed_map").blocking()

# Standard Put and Get
distributed_map.put("key", "value")
distributed_map.get("key")

# Concurrent Map methods, optimistic updating
distributed_map.put_if_absent("some key", "some value")
distributed_map.replace_if_same("key", "value", "new value")

# Shutdown this Hazelcast Client
client.shutdown()
