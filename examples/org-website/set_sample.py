import hazelcast
import logging

if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
    logging.getLogger().setLevel(logging.INFO)

    # Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
    hz = hazelcast.HazelcastClient()
    # Get the Distributed Set from Cluster.
    mset = hz.get_set("my-distributed-set")
    # Add items to the set with duplicates
    mset.add("item1")
    mset.add("item1")
    mset.add("item2")
    mset.add("item2")
    mset.add("item2")
    mset.add("item3")
    # Get the items. Note that there are no duplicates.
    print(mset.get_all().result())
    # Shutdown this Hazelcast Client
    hz.shutdown()
