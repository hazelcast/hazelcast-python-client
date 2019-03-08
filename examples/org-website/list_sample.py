import hazelcast

if __name__ == "__main__":
    # Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
    hz = hazelcast.HazelcastClient()
    # Get the Distributed List from Cluster.
    list = hz.get_list("my-distributed-list").blocking()
    # Add element to the list
    list.add("item1")
    list.add("item2")

    # Remove the first element
    print("Removed: {}".format(list.remove_at(0)))
    # There is only one element left
    print("Current size is {}".format(list.size()))
    # Clear the list
    list.clear()
    # Shutdown this Hazelcast Client
    hz.shutdown()
