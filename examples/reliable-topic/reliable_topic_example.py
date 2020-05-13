import time

import hazelcast
from hazelcast import ClientConfig
from hazelcast.proxy.reliable_topic import ReliableMessageListener


class MyReliableMessageListener(ReliableMessageListener):
    def on_message(self, event):
        print("Got message: {}".format(event.message))
        print("Publish time: {}\n".format(event.publish_time))


if __name__ == "__main__":
    config = ClientConfig()
    config.set_property("hazelcast.serialization.input.returns.bytearray", True)
    client = hazelcast.HazelcastClient(config)
    listener = MyReliableMessageListener()

    reliable_topic = client.get_reliable_topic("reliable-topic")
    registration_id = reliable_topic.add_listener(listener)

    for i in range(10):
        reliable_topic.publish("Message " + str(i))
        time.sleep(0.1)

    reliable_topic.destroy()
    reliable_topic.remove_listener(registration_id)
    client.shutdown()
