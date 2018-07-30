import hazelcast
import logging
import threading

if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

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

    produce_thread = threading.Thread(target=produce)
    consume_thread = threading.Thread(target=consume)

    produce_thread.start()
    consume_thread.start()

    produce_thread.join()
    consume_thread.join()

    client.shutdown()
