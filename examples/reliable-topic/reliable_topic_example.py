import hazelcast
from hazelcast.config import TopicOverloadPolicy
from hazelcast.proxy.reliable_topic import ReliableMessageListener

# Customize the reliable topic
client = hazelcast.HazelcastClient(
    reliable_topics={
        "my-topic": {
            "overload_policy": TopicOverloadPolicy.DISCARD_OLDEST,
            "read_batch_size": 20,
        }
    }
)

topic = client.get_reliable_topic("my-topic").blocking()

# Add a listener with a callable
reg_id = topic.add_listener(lambda m: print("First listener:", m))


# Or, customize the behaviour of the listener
# via ReliableMessageListener
class MyListener(ReliableMessageListener):
    def on_message(self, message):
        print("Second listener:", message)

    def retrieve_initial_sequence(self):
        print("Listener function retrieve_initial_sequence is called")
        return 0

    def store_sequence(self, sequence):
        print("Listener function store_sequence is called with sequence: ", sequence)
        pass

    def is_loss_tolerant(self) -> bool:
        print("Listener function is_loss_tolerant is called")
        return True

    def is_terminal(self, error):
        print("Listener function is_terminal is called with error: ", error)
        return False

    def on_cancel(self):
        print("Listener function on_cancel is called")


# Add a custom ReliableMessageListener
topic.add_listener(MyListener())


for i in range(100):
    # Publish messages one-by-one
    topic.publish(i)


messages = range(100, 200)

# Publish message in batch
topic.publish_all(messages)

# Remove listener so that it won't receive
# messages anymore
topic.remove_listener(reg_id)

client.shutdown()
