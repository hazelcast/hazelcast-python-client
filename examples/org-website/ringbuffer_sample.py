import hazelcast

# Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
client = hazelcast.HazelcastClient()
rb = client.get_ringbuffer("rb").blocking()
# add two items into ring buffer
rb.add(100)
rb.add(200)
# we start from the oldest item.
# if you want to start from the next item, call rb.tailSequence()+1
sequence = rb.head_sequence()
print(rb.read_one(sequence))
sequence += 1
print(rb.read_one(sequence))
# Shutdown this Hazelcast Client
client.shutdown()
