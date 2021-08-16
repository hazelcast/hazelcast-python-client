import os
import unittest

from tests.hzrc.ttypes import Lang

try:
    from hazelcast.config import TopicOverloadPolicy
    from hazelcast.errors import TopicOverloadError
    from hazelcast.proxy.reliable_topic import ReliableMessageListener
except ImportError:
    # For backward compatibility. If we cannot import those, we won't
    # be even referencing them in tests.
    pass

from tests.base import SingleMemberTestCase
from tests.util import (
    is_client_version_older_than,
    random_string,
    event_collector,
    get_current_timestamp,
    mark_client_version_at_least,
)

CAPACITY = 10


@unittest.skipIf(
    is_client_version_older_than("4.1"), "Tests the features added in 4.1 version of the client"
)
class ReliableTopicTest(SingleMemberTestCase):
    @classmethod
    def configure_cluster(cls):
        path = os.path.abspath(__file__)
        dir_path = os.path.dirname(path)
        with open(os.path.join(dir_path, "hazelcast_topic.xml")) as f:
            return f.read()

    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        if not is_client_version_older_than("4.1"):
            # Add these config elements only to the 4.1+ clients
            # since the older versions do not know anything
            # about them.
            config["reliable_topics"] = {
                "discard": {
                    "overload_policy": TopicOverloadPolicy.DISCARD_NEWEST,
                },
                "overwrite": {
                    "overload_policy": TopicOverloadPolicy.DISCARD_OLDEST,
                },
                "block": {
                    "overload_policy": TopicOverloadPolicy.BLOCK,
                },
                "error": {
                    "overload_policy": TopicOverloadPolicy.ERROR,
                },
            }
        return config

    def setUp(self):
        self.topics = []
        self.topic = self.get_topic(random_string())

    def tearDown(self):
        for topic in self.topics:
            topic.destroy()

    def test_add_listener_with_function(self):
        topic = self.get_topic(random_string())

        collector = event_collector()
        registration_id = topic.add_listener(collector)
        self.assertIsNotNone(registration_id)

        topic.publish("a")
        topic.publish("b")

        self.assertTrueEventually(
            lambda: self.assertEqual(["a", "b"], list(map(lambda m: m.message, collector.events)))
        )

    def test_add_listener(self):
        topic = self.get_topic(random_string())

        messages = []

        class Listener(ReliableMessageListener):
            def on_message(self, message):
                messages.append(message.message)

            def retrieve_initial_sequence(self):
                return -1

            def store_sequence(self, sequence):
                pass

            def is_loss_tolerant(self):
                return False

            def is_terminal(self, error):
                return False

        registration_id = topic.add_listener(Listener())
        self.assertIsNotNone(registration_id)

        topic.publish("a")
        topic.publish("b")

        self.assertTrueEventually(lambda: self.assertEqual(["a", "b"], messages))

    def test_add_listener_with_retrieve_initial_sequence(self):
        topic = self.get_topic(random_string())

        messages = []

        class Listener(ReliableMessageListener):
            def on_message(self, message):
                messages.append(message.message)

            def retrieve_initial_sequence(self):
                return 5

            def store_sequence(self, sequence):
                pass

            def is_loss_tolerant(self):
                return False

            def is_terminal(self, error):
                return False

        topic.publish_all(range(10))

        registration_id = topic.add_listener(Listener())
        self.assertIsNotNone(registration_id)

        self.assertTrueEventually(lambda: self.assertEqual(list(range(5, 10)), messages))

    def test_add_listener_with_store_sequence(self):
        topic = self.get_topic(random_string())

        sequences = []

        class Listener(ReliableMessageListener):
            def on_message(self, message):
                pass

            def retrieve_initial_sequence(self):
                return -1

            def store_sequence(self, sequence):
                sequences.append(sequence)

            def is_loss_tolerant(self):
                return False

            def is_terminal(self, error):
                return False

        registration_id = topic.add_listener(Listener())
        self.assertIsNotNone(registration_id)

        topic.publish_all(["item-%s" % i for i in range(20)])

        self.assertTrueEventually(lambda: self.assertEqual(list(range(20)), sequences))

    def test_add_listener_with_loss_tolerant_listener_on_message_loss(self):
        topic = self.get_topic("overwrite")  # has capacity of 10

        messages = []

        class Listener(ReliableMessageListener):
            def on_message(self, message):
                messages.append(message.message)

            def retrieve_initial_sequence(self):
                return -1

            def store_sequence(self, sequence):
                pass

            def is_loss_tolerant(self):
                return True

            def is_terminal(self, error):
                return False

        registration_id = topic.add_listener(Listener())
        self.assertIsNotNone(registration_id)

        # will overwrite first 10 messages, hence they will be lost
        topic.publish_all(range(2 * CAPACITY))

        self.assertTrueEventually(
            lambda: self.assertEqual(list(range(CAPACITY, 2 * CAPACITY)), messages)
        )

    def test_add_listener_with_non_loss_tolerant_listener_on_message_loss(self):
        topic = self.get_topic("overwrite")  # has capacity of 10

        messages = []

        class Listener(ReliableMessageListener):
            def on_message(self, message):
                messages.append(message.message)

            def retrieve_initial_sequence(self):
                return -1

            def store_sequence(self, sequence):
                pass

            def is_loss_tolerant(self):
                return False

            def is_terminal(self, error):
                return False

        registration_id = topic.add_listener(Listener())
        self.assertIsNotNone(registration_id)

        # will overwrite first 10 messages, hence they will be lost
        topic.publish_all(range(2 * CAPACITY))

        self.assertEqual(0, len(messages))

        # Should be cancelled on message loss
        self.assertTrueEventually(lambda: self.assertEqual(0, len(topic._runners)))

    def test_add_listener_when_on_message_raises_error(self):
        topic = self.get_topic(random_string())

        messages = []

        class Listener(ReliableMessageListener):
            def on_message(self, message):
                message = message.message
                if message < 5:
                    messages.append(message)
                else:
                    raise ValueError("expected")

            def retrieve_initial_sequence(self):
                return -1

            def store_sequence(self, sequence):
                pass

            def is_loss_tolerant(self):
                return False

            def is_terminal(self, error):
                return isinstance(error, ValueError)

        registration_id = topic.add_listener(Listener())
        self.assertIsNotNone(registration_id)

        topic.publish_all(range(10))

        self.assertTrueEventually(lambda: self.assertEqual(list(range(5)), messages))

        # Should be cancelled since on_message raised error
        self.assertTrueEventually(lambda: self.assertEqual(0, len(topic._runners)))

    def test_add_listener_when_on_message_and_is_terminal_raises_error(self):
        topic = self.get_topic(random_string())

        messages = []

        class Listener(ReliableMessageListener):
            def on_message(self, message):
                message = message.message
                if message < 5:
                    messages.append(message)
                else:
                    raise ValueError("expected")

            def retrieve_initial_sequence(self):
                return -1

            def store_sequence(self, sequence):
                pass

            def is_loss_tolerant(self):
                return False

            def is_terminal(self, error):
                raise error

        registration_id = topic.add_listener(Listener())
        self.assertIsNotNone(registration_id)

        topic.publish_all(range(10))

        self.assertTrueEventually(lambda: self.assertEqual(list(range(5)), messages))

        # Should be cancelled since on_message raised error
        self.assertTrueEventually(lambda: self.assertEqual(0, len(topic._runners)))

    def test_add_listener_with_non_callable(self):
        topic = self.get_topic(random_string())
        with self.assertRaises(TypeError):
            topic.add_listener(3)

    def test_remove_listener(self):
        topic = self.get_topic(random_string())

        registration_id = topic.add_listener(lambda m: m)
        self.assertTrue(topic.remove_listener(registration_id))

    def test_remove_listener_does_not_receive_messages_after_removal(self):
        topic = self.get_topic(random_string())

        collector = event_collector()
        registration_id = topic.add_listener(collector)
        self.assertTrue(topic.remove_listener(registration_id))

        topic.publish_all(range(10))

        self.assertEqual(0, len(collector.events))

    def test_remove_listener_twice(self):
        topic = self.get_topic(random_string())
        registration_id = topic.add_listener(lambda m: m)
        self.assertTrue(topic.remove_listener(registration_id))
        self.assertFalse(topic.remove_listener(registration_id))

    def test_publish_with_discard_newest_policy(self):
        topic = self.get_topic("discard")

        collector = event_collector()
        topic.add_listener(collector)

        for i in range(2 * CAPACITY):
            topic.publish(i)

        self.assertTrueEventually(lambda: self.assertEqual(CAPACITY, len(collector.events)))
        self.assertEqual(list(range(CAPACITY)), self.get_ringbuffer_data(topic))

    def test_publish_with_discard_oldest_policy(self):
        topic = self.get_topic("overwrite")

        collector = event_collector()
        topic.add_listener(collector)

        for i in range(2 * CAPACITY):
            topic.publish(i)

        self.assertTrueEventually(lambda: self.assertEqual(2 * CAPACITY, len(collector.events)))
        self.assertEqual(list(range(CAPACITY, 2 * CAPACITY)), self.get_ringbuffer_data(topic))

    def test_publish_with_block_policy(self):
        topic = self.get_topic("block")

        collector = event_collector()
        topic.add_listener(collector)

        for i in range(CAPACITY):
            topic.publish(i)

        begin_time = get_current_timestamp()

        for i in range(CAPACITY, 2 * CAPACITY):
            topic.publish(i)

        time_passed = get_current_timestamp() - begin_time

        # TTL is set in the XML config
        self.assertTrue(time_passed >= 2.0)

        self.assertTrueEventually(lambda: self.assertEqual(2 * CAPACITY, len(collector.events)))
        self.assertEqual(list(range(CAPACITY, CAPACITY * 2)), self.get_ringbuffer_data(topic))

    def test_publish_with_error_policy(self):
        topic = self.get_topic("error")

        collector = event_collector()
        topic.add_listener(collector)

        for i in range(CAPACITY):
            topic.publish(i)

        for i in range(CAPACITY, 2 * CAPACITY):
            with self.assertRaises(TopicOverloadError):
                topic.publish(i)

        self.assertTrueEventually(lambda: self.assertEqual(CAPACITY, len(collector.events)))
        self.assertEqual(list(range(CAPACITY)), self.get_ringbuffer_data(topic))

    def test_publish_all_with_discard_newest_policy(self):
        topic = self.get_topic("discard")

        collector = event_collector()
        topic.add_listener(collector)

        topic.publish_all(range(CAPACITY))
        topic.publish_all(range(CAPACITY, 2 * CAPACITY))

        self.assertTrueEventually(lambda: self.assertEqual(CAPACITY, len(collector.events)))
        self.assertEqual(list(range(CAPACITY)), self.get_ringbuffer_data(topic))

    def test_publish_all_with_discard_oldest_policy(self):
        topic = self.get_topic("overwrite")

        collector = event_collector()
        topic.add_listener(collector)

        topic.publish_all(range(CAPACITY))
        topic.publish_all(range(CAPACITY, 2 * CAPACITY))

        self.assertTrueEventually(lambda: self.assertEqual(2 * CAPACITY, len(collector.events)))
        self.assertEqual(list(range(CAPACITY, 2 * CAPACITY)), self.get_ringbuffer_data(topic))

    def test_publish_all_with_block_policy(self):
        topic = self.get_topic("block")

        collector = event_collector()
        topic.add_listener(collector)

        topic.publish_all(range(CAPACITY))

        begin_time = get_current_timestamp()
        topic.publish_all(range(CAPACITY, 2 * CAPACITY))
        time_passed = get_current_timestamp() - begin_time

        # TTL is set in the XML config
        self.assertTrue(time_passed >= 2.0)

        self.assertTrueEventually(lambda: self.assertEqual(2 * CAPACITY, len(collector.events)))
        self.assertEqual(list(range(CAPACITY, CAPACITY * 2)), self.get_ringbuffer_data(topic))

    def test_publish_all_with_error_policy(self):
        topic = self.get_topic("error")

        collector = event_collector()
        topic.add_listener(collector)

        topic.publish_all(range(CAPACITY))

        with self.assertRaises(TopicOverloadError):
            topic.publish_all(range(CAPACITY, 2 * CAPACITY))

        self.assertTrueEventually(lambda: self.assertEqual(CAPACITY, len(collector.events)))
        self.assertEqual(list(range(CAPACITY)), self.get_ringbuffer_data(topic))

    def test_durable_subscription(self):
        topic = self.get_topic(random_string())

        class DurableListener(ReliableMessageListener):
            def __init__(self):
                self.objects = []
                self.sequences = []
                self.sequence = -1

            def on_message(self, message):
                self.objects.append(message.message)

            def retrieve_initial_sequence(self):
                if self.sequence == -1:
                    return self.sequence

                # +1 to read the next item
                return self.sequence + 1

            def store_sequence(self, sequence):
                self.sequences.append(sequence)
                self.sequence = sequence

            def is_loss_tolerant(self):
                return False

            def is_terminal(self, error):
                return True

        listener = DurableListener()

        registration_id = topic.add_listener(listener)
        topic.publish("item1")

        self.assertTrueEventually(lambda: self.assertEqual(["item1"], listener.objects))

        self.assertTrue(topic.remove_listener(registration_id))

        topic.publish("item2")
        topic.publish("item3")

        topic.add_listener(listener)

        def assertion():
            self.assertEqual(["item1", "item2", "item3"], listener.objects)
            self.assertEqual([0, 1, 2], listener.sequences)

        self.assertTrueEventually(assertion)

    def test_client_receives_when_server_publish_messages(self):
        mark_client_version_at_least(self, "4.2.1")

        topic_name = random_string()
        topic = self.get_topic(topic_name)

        received_message_count = [0]

        def listener(message):
            self.assertIsNotNone(message.member)
            received_message_count[0] += 1

        topic.add_listener(listener)

        message_count = 10

        script = """
        var topic = instance_0.getReliableTopic("%s");
        for (var i = 0; i < %d; i++) {
            topic.publish(i);
        }
        """ % (
            topic_name,
            message_count,
        )

        self.rc.executeOnController(self.cluster.id, script, Lang.JAVASCRIPT)
        self.assertTrueEventually(
            lambda: self.assertEqual(message_count, received_message_count[0])
        )

    def get_ringbuffer_data(self, topic):
        ringbuffer = topic._ringbuffer
        return list(
            map(
                lambda m: topic._to_object(m.payload),
                ringbuffer.read_many(
                    ringbuffer.head_sequence().result(), CAPACITY, CAPACITY
                ).result(),
            )
        )

    def get_topic(self, name):
        topic = self.client.get_reliable_topic(name)
        self.topics.append(topic)
        return topic.blocking()
