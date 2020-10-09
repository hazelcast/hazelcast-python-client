import hazelcast

client = hazelcast.HazelcastClient()

rb = client.get_ringbuffer("ring-buffer").blocking()
print("Capacity of the ring buffer:", rb.capacity())

sequence = rb.add("First item")
print("Size:", rb.size())

item = rb.read_one(sequence)
print("The item at the sequence %s is %s" % (sequence, item))

client.shutdown()
