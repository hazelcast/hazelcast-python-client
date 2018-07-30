import hazelcast
import logging
import time


def on_message(event):
    print("Got message: {}".format(event.message))
    print("Publish time: {}\n".format(event.publish_time))


if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    client = hazelcast.HazelcastClient()

    topic = client.get_topic("topic")
    topic.add_listener(on_message)

    for i in range(10):
        topic.publish("Message " + str(i))
        time.sleep(0.1)

    client.shutdown()
