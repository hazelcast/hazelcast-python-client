import hazelcast


def print_on_message(topic_message):
    print("Got message", topic_message.message)


# Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
client = hazelcast.HazelcastClient()
# Get a Topic called "my-distributed-topic"
topic = client.get_topic("my-distributed-topic").blocking()
# Add a Listener to the Topic
topic.add_listener(print_on_message)
# Publish a message to the Topic
topic.publish("Hello to distributed world")
# Shutdown this Hazelcast Client
client.shutdown()
