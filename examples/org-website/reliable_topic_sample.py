import hazelcast

# Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
client = hazelcast.HazelcastClient()

# Get a Topic called "my-distributed-topic"
topic = client.get_reliable_topic("my-distributed-topic").blocking()

# Add a Listener to the Topic
topic.add_listener(lambda message: print(message))

# Publish a message to the Topic
topic.publish("Hello to distributed world")

# Shutdown this Hazelcast Client
client.shutdown()
