import logging
import hazelcast

logging.basicConfig(level=logging.INFO)

client = hazelcast.HazelcastClient()

topic = client.get_topic("topic").blocking()


def on_message(event):
    print(f"Got message: {event.message}")
    print(f"Publish time: {event.publish_time}")


topic.add_listener(on_message)

for i in range(10):
    topic.publish(f"Message {i}")

client.shutdown()
