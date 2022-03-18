import logging
import hazelcast

from hazelcast.config import TopicOverloadPolicy
from hazelcast.proxy.reliable_topic import ReliableMessageListener

logging.basicConfig(level=logging.INFO)

# Customize the reliable topic
client = hazelcast.HazelcastClient(
    reliable_topics={
        "my-topic": {
            "overload_policy": TopicOverloadPolicy.DISCARD_OLDEST,
            "read_batch_size": 20,
        }
    }
)

reliable_topic = client.get_reliable_topic("reliable_topic").blocking()

# Add a listener with a callable
reg_id = reliable_topic.add_listener(lambda m: print("First listener:", m))


# Or, customize the behaviour of the listener
# via ReliableMessageListener
class MyListener(ReliableMessageListener):
    def on_message(self, message):
        print("Second listener:", message)

    def retrieve_initial_sequence(self):
        return 0

    def store_sequence(self, sequence):
        pass

    def is_loss_tolerant(self):
        return True

    def is_terminal(self, error):
        return False


# Add a custom ReliableMessageListener
reliable_topic.add_listener(MyListener())


for i in range(100):
    # Publish messages one-by-one
    reliable_topic.publish(i)


messages = range(100, 200)

# Publish message in batch
reliable_topic.publish_all(messages)

# Remove listener so that it won't receive
# messages anymore
reliable_topic.remove_listener(reg_id)

client.shutdown()
