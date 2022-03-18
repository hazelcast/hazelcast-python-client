import logging
import hazelcast

logging.basicConfig(level=logging.INFO)

# Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
client = hazelcast.HazelcastClient()

# Get a Replicated Map called "replicated_map"
replicated_map = client.get_replicated_map("replicated_map").blocking()

# Put and Get a value from the Replicated Map
# key/value replicated to all members
replaced_value = replicated_map.put("key", "value")

# Will be None as its first update
print(f"replaced value = {replaced_value}")

# the value is retrieved from a random member in the cluster
value = replicated_map.get("key")
print(f"value for key = {value}")

# Shutdown this Hazelcast Client
client.shutdown()
