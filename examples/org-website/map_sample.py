import hazelcast

# Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
hz = hazelcast.HazelcastClient()
# Get the Distributed Map from Cluster.
my_map = hz.get_map("my-distributed-map").blocking()
# Standard Put and Get
my_map.put("key", "value")
my_map.get("key")
# Concurrent Map methods, optimistic updating
my_map.put_if_absent("somekey", "somevalue")
my_map.replace_if_same("key", "value", "newvalue")
# Shutdown this Hazelcast Client
hz.shutdown()
