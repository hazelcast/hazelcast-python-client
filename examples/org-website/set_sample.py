import logging
import hazelcast

logging.basicConfig(level=logging.INFO)

# Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
client = hazelcast.HazelcastClient()

# Get the Distributed Set from Cluster.
distributed_set = client.get_set("distributed_set").blocking()

# Add items to the set with duplicates
distributed_set.add("item1")
distributed_set.add("item1")
distributed_set.add("item2")
distributed_set.add("item2")
distributed_set.add("item2")
distributed_set.add("item3")

# Get the items. Note that there are no duplicates.
for item in distributed_set.get_all():
    print(item)

# Shutdown this Hazelcast Client
client.shutdown()
