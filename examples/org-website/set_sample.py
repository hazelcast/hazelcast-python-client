import hazelcast

# Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
hz = hazelcast.HazelcastClient()
# Get the Distributed Set from Cluster.
my_set = hz.get_set("my-distributed-set").blocking()
# Add items to the set with duplicates
my_set.add("item1")
my_set.add("item1")
my_set.add("item2")
my_set.add("item2")
my_set.add("item2")
my_set.add("item3")
# Get the items. Note that there are no duplicates.
for item in my_set.get_all():
    print(item)
# Shutdown this Hazelcast Client
hz.shutdown()
