import hazelcast
import threading

client = hazelcast.HazelcastClient()

queue = client.get_queue("queue")


def produce():
    for i in range(100):
        queue.offer("value-" + str(i))


def consume():
    consumed_count = 0
    while consumed_count < 100:
        head = queue.take().result()
        print("Consuming {}".format(head))
        consumed_count += 1


producer_thread = threading.Thread(target=produce)
consumer_thread = threading.Thread(target=consume)

producer_thread.start()
consumer_thread.start()

producer_thread.join()
consumer_thread.join()

client.shutdown()
