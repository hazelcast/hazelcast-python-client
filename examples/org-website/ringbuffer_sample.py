import logging
import hazelcast

logging.basicConfig(level=logging.INFO)

# Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
client = hazelcast.HazelcastClient()

ringbuffer = client.get_ringbuffer("ringbuffer").blocking()

# add two items into ring buffer
ringbuffer.add(100)
ringbuffer.add(200)

# we start from the oldest item.
# if you want to start from the next item, call rb.tailSequence()+1
sequence = ringbuffer.head_sequence()
print(ringbuffer.read_one(sequence))

sequence += 1
print(ringbuffer.read_one(sequence))

# Shutdown this Hazelcast Client
client.shutdown()
