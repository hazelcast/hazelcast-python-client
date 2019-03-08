import hazelcast

if __name__ == "__main__":
    client = hazelcast.HazelcastClient()

    ring_buffer = client.get_ringbuffer("ring-buffer")
    print("Capacity of the ring buffer: {}".format(ring_buffer.capacity().result()))

    sequence = ring_buffer.add("First item").result()
    print("Size: {}".format(ring_buffer.size().result()))

    item = ring_buffer.read_one(sequence).result()
    print("The item at the sequence {} is {}".format(sequence, item))

    client.shutdown()
