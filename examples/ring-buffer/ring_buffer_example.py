import logging
import hazelcast

logging.basicConfig(level=logging.INFO)

client = hazelcast.HazelcastClient()

ringbuffer = client.get_ringbuffer("ringbuffer").blocking()

print(f"Capacity of the ring buffer: {ringbuffer.capacity()}")

sequence = ringbuffer.add("First item")
print(f"Size: {ringbuffer.size()}")

item = ringbuffer.read_one(sequence)
print(f"The item at the sequence {sequence} is {item}")

client.shutdown()
