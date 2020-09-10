import hazelcast

# Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
hz = hazelcast.HazelcastClient()
# Get the Distributed List from Cluster.
my_list = hz.get_list("my-distributed-list").blocking()
# Add element to the list
my_list.add("item1")
my_list.add("item2")

# Remove the first element
print("Removed:", my_list.remove_at(0))
# There is only one element left
print("Current size is", my_list.size())
# Clear the list
my_list.clear()
# Shutdown this Hazelcast Client
hz.shutdown()
