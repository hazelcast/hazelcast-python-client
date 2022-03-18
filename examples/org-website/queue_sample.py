import logging
import hazelcast

logging.basicConfig(level=logging.INFO)

# Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
client = hazelcast.HazelcastClient()

# Get a Blocking Queue called "distributed_queue"
distributed_queue = client.get_queue("distributed_queue").blocking()

# Offer a String into the Distributed Queue
distributed_queue.offer("item")

# Poll the Distributed Queue and return the String
item = distributed_queue.poll()

# Timed blocking Operations
distributed_queue.offer("another item", 0.5)
another_item = distributed_queue.poll(5)

# Indefinitely blocking Operations
distributed_queue.put("yet another item")
print(distributed_queue.take())

# Shutdown this Hazelcast Client
client.shutdown()
