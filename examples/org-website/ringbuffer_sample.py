import hazelcast
import logging

if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
    logging.getLogger().setLevel(logging.INFO)

    # Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
    hz = hazelcast.HazelcastClient()
    rb = hz.get_ringbuffer("rb")
    # add two items into ring buffer
    rb.add(100).result()
    rb.add(200).result()
    # we start from the oldest item.
    # if you want to start from the next item, call rb.tailSequence()+1
    sequence = rb.head_sequence().result()
    print(rb.read_one(sequence).result())
    sequence += 1
    print(rb.read_one(sequence).result())
    # Shutdown this Hazelcast Client
    hz.shutdown()
