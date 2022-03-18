import logging
import hazelcast

logging.basicConfig(level=logging.INFO)

# Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
client = hazelcast.HazelcastClient()

# Get the Distributed List from Cluster.
distributed_list = client.get_list("distributed_list").blocking()

# Add element to the list
distributed_list.add("item1")
distributed_list.add("item2")

# Remove the first element
print(f"Removed: {distributed_list.remove_at(0)}")

# There is only one element left
print(f"Current size is {distributed_list.size()}")

# Clear the list
distributed_list.clear()

# Shutdown this Hazelcast Client
client.shutdown()
