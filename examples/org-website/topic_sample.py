import hazelcast
import logging
from hazelcast import six


def print_on_message(topic_message):
    six.print_("Got message ", topic_message.message)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
    logging.getLogger().setLevel(logging.INFO)
    logger = logging.getLogger("main")

    # Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
    hz = hazelcast.HazelcastClient()
    # Get a Topic called "my-distributed-topic"
    topic = hz.get_topic("my-distributed-topic")
    # Add a Listener to the Topic
    topic.add_listener(print_on_message)
    # Publish a message to the Topic
    topic.publish("Hello to distributed world")
    # Shutdown this Hazelcast Client
    hz.shutdown()
