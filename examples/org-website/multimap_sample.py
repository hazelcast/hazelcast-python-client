import hazelcast

# Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
client = hazelcast.HazelcastClient()
# Get the Distributed MultiMap from Cluster.
multi_map = client.get_multi_map("my-distributed-multimap").blocking()
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
client.shutdown()
