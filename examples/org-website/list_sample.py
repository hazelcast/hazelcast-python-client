import hazelcast
import logging

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
    logging.getLogger().setLevel(logging.INFO)

    # Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
    hz = hazelcast.HazelcastClient()
    # Get the Distributed List from Cluster.
    mlist = hz.get_list("my-distributed-list")
    # Add element to the list
    mlist.add("item1")
    mlist.add("item2")

    # Remove the first element
    print("Removed: {}".format(mlist.remove_at(0).result()))
    # There is only one element left
    print("Current size is {}".format(mlist.size().result()))
    # Clear the list
    mlist.clear()
    # Shutdown this Hazelcast Client
    hz.shutdown()
