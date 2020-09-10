import hazelcast
import time


def on_message(event):
    print("Got message:", event.message)
    print("Publish time:", event.publish_time)


client = hazelcast.HazelcastClient()

topic = client.get_topic("topic")
topic.add_listener(on_message)

for i in range(10):
    topic.publish("Message " + str(i))
    time.sleep(0.1)

client.shutdown()
