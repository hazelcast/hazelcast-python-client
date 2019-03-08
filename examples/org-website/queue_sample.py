import hazelcast

if __name__ == "__main__":
    # Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
    hz = hazelcast.HazelcastClient()
    # Get a Blocking Queue called "my-distributed-queue"
    queue = hz.get_queue("my-distributed-queue").blocking()
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
    hz.shutdown()
