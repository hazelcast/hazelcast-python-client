import hazelcast

# Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
hz = hazelcast.HazelcastClient()
# Get the Distributed Map from Cluster.
map = hz.get_map("my-distributed-map").blocking()
# Standard Put and Get
map.put("key", "value")
map.get("key")
# Concurrent Map methods, optimistic updating
map.put_if_absent("somekey", "somevalue")
map.replace_if_same("key", "value", "newvalue")
# Shutdown this Hazelcast Client
hz.shutdown()
