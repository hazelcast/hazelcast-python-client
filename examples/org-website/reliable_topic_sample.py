import logging
import hazelcast

logging.basicConfig(level=logging.INFO)

# Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
client = hazelcast.HazelcastClient()

# Get a Topic called "distributed_topic"
distributed_topic = client.get_reliable_topic("distributed_topic").blocking()

# Add a Listener to the Topic
distributed_topic.add_listener(lambda message: print(message))

# Publish a message to the Topic
distributed_topic.publish("Hello to distributed world")

# Shutdown this Hazelcast Client
client.shutdown()
