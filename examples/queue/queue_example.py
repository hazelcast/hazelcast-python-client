import logging
import hazelcast
import threading

logging.basicConfig(level=logging.INFO)

client = hazelcast.HazelcastClient()

queue = client.get_queue("queue").blocking()


def produce():
    for i in range(100):
        queue.offer(f"value-{i}")


def consume():
    consumed_count = 0
    while consumed_count < 100:
        head = queue.take()
        print(f"Consuming {head}")
        consumed_count += 1


producer_thread = threading.Thread(target=produce)
consumer_thread = threading.Thread(target=consume)

producer_thread.start()
consumer_thread.start()

producer_thread.join()
consumer_thread.join()

client.shutdown()
