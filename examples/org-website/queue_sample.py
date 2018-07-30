import hazelcast
import logging

if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
    logging.getLogger().setLevel(logging.INFO)

    # Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
    hz = hazelcast.HazelcastClient()
    # Get a Blocking Queue called "my-distributed-queue"
    queue = hz.get_queue("my-distributed-queue")
    # Offer a String into the Distributed Queue
    queue.offer("item")
    # Poll the Distributed Queue and return the String
    item = queue.poll().result()
    # Timed blocking Operations
    queue.offer("anotheritem", 1).result()
    anotherItem = queue.poll(5).result()
    # Indefinitely blocking Operations
    queue.put("yetanotheritem").result()
    print(queue.take().result())
    # Shutdown this Hazelcast Client
    hz.shutdown()
