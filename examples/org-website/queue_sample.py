import hazelcast

# Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
client = hazelcast.HazelcastClient()
# Get a Blocking Queue called "my-distributed-queue"
queue = client.get_queue("my-distributed-queue").blocking()
# Offer a String into the Distributed Queue
queue.offer("item")
# Poll the Distributed Queue and return the String
item = queue.poll()
# Timed blocking Operations
queue.offer("anotheritem", 0.5)
another_item = queue.poll(5)
# Indefinitely blocking Operations
queue.put("yetanotheritem")
print(queue.take())
# Shutdown this Hazelcast Client
client.shutdown()
