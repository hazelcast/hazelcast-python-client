import os
import time
from datetime import datetime

import hazelcast
from hazelcast import ClientConfig
from hazelcast.config import ReliableTopicConfig, TOPIC_OVERLOAD_POLICY
from hazelcast.exception import IllegalArgumentError, TopicOverflowError
from hazelcast.proxy.reliable_topic import ReliableMessageListener
from hazelcast.proxy.ringbuffer import OVERFLOW_POLICY_FAIL, OVERFLOW_POLICY_OVERWRITE
from hazelcast.serialization.reliable_topic import ReliableTopicMessage
from hazelcast.util import current_time_in_millis
from tests.base import SingleMemberTestCase
from tests.util import random_string, event_collector


class _ReliableTopicTestException(BaseException):
    pass


class TestReliableMessageListener(ReliableMessageListener):
    def __init__(self, collector):
        self._collector = collector

    def on_message(self, event):
        self._collector(event)


class ReliableTopicTest(SingleMemberTestCase):
    @classmethod
    def configure_cluster(cls):
        path = os.path.abspath(__file__)
        dir_path = os.path.dirname(path)
        with open(os.path.join(dir_path, "hazelcast_topic.xml")) as f:
            return f.read()

    def setUp(self):
        config = ClientConfig()
        config.set_property("hazelcast.serialization.input.returns.bytearray", True)

        discard_config = ReliableTopicConfig("discard")
        discard_config.topic_overload_policy = TOPIC_OVERLOAD_POLICY.DISCARD_NEWEST
        config.add_reliable_topic_config(discard_config)

        overwrite_config = ReliableTopicConfig("overwrite")
        overwrite_config.topic_overload_policy = TOPIC_OVERLOAD_POLICY.DISCARD_OLDEST
        config.add_reliable_topic_config(overwrite_config)

        error_config = ReliableTopicConfig("error")
        error_config.topic_overload_policy = TOPIC_OVERLOAD_POLICY.ERROR
        config.add_reliable_topic_config(error_config)

        stale_config = ReliableTopicConfig("stale")
        stale_config.topic_overload_policy = TOPIC_OVERLOAD_POLICY.DISCARD_OLDEST
        config.add_reliable_topic_config(stale_config)

        self.client = hazelcast.HazelcastClient(self.configure_client(config))
        self.reliable_topic = self.client.get_reliable_topic(random_string()).blocking()
        self.registration_id = None

    def tearDown(self):
        if self.registration_id is not None:
            self.reliable_topic.remove_listener(self.registration_id)

    def test_add_listener(self):
        collector = event_collector()
        reliable_listener = TestReliableMessageListener(collector)
        self.registration_id = self.reliable_topic.add_listener(reliable_listener)
        self.reliable_topic.publish('item-value')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEqual(event.message, 'item-value')
            self.assertGreater(event.publish_time, 0)

        self.assertTrueEventually(assert_event, 5)

    def test_remove_listener(self):
        collector = event_collector()
        reliable_listener = TestReliableMessageListener(collector)

        reg_id = self.reliable_topic.add_listener(reliable_listener)
        removed = self.reliable_topic.remove_listener(reg_id)
        self.assertTrue(removed, True)

    def test_none_listener(self):
        with self.assertRaises(IllegalArgumentError):
            self.reliable_topic.add_listener("invalid-listener")

    def test_remove_listener_when_does_not_exist(self):
        with self.assertRaises(IllegalArgumentError):
            self.reliable_topic.remove_listener("id")

    def test_remove_listener_when_already_removed(self):
        collector = event_collector()
        reliable_listener = TestReliableMessageListener(collector)

        reg_id = self.reliable_topic.add_listener(reliable_listener)
        self.reliable_topic.remove_listener(reg_id)

        with self.assertRaises(IllegalArgumentError):
            self.reliable_topic.remove_listener(reg_id)

    def test_error_on_message_not_terminal(self):
        collector = event_collector()

        class TestReliableMessageListenerNotTerminal(ReliableMessageListener):
            def __init__(self, _collector):
                self._collector = _collector

            def on_message(self, event):
                if event.message == "raise-exception":
                    raise _ReliableTopicTestException("test-exception")

                self._collector(event)

            def is_terminal(self):
                return False

        reliable_listener = TestReliableMessageListenerNotTerminal(collector)
        self.registration_id = self.reliable_topic.add_listener(reliable_listener)

        self.reliable_topic.publish('raise-exception')
        self.reliable_topic.publish('work-normally')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEqual(event.message, 'work-normally')
            self.assertGreater(event.publish_time, 0)

        self.assertTrueEventually(assert_event, 5)

    def test_error_on_message_terminal(self):
        collector = event_collector()

        class TestReliableMessageListenerTerminal(ReliableMessageListener):
            def __init__(self, _collector):
                self._collector = _collector

            def on_message(self, event):
                if event.message == "raise-exception":
                    raise _ReliableTopicTestException("test-exception")

                self._collector(event)

            def is_terminal(self):
                return True

        reliable_listener = TestReliableMessageListenerTerminal(collector)
        # This listener will be removed by the ReliableTopic implementation
        self.reliable_topic.add_listener(reliable_listener)

        self.reliable_topic.publish('raise-exception')
        self.reliable_topic.publish('work-normally')
        time.sleep(0.5)
        self.assertEqual(len(collector.events), 0)

    def test_error_on_message_terminal_exception(self):
        collector = event_collector()

        class TestReliableMessageListenerTerminal(ReliableMessageListener):
            def __init__(self, _collector):
                self._collector = _collector

            def on_message(self, event):
                if event.message == "raise-exception":
                    raise _ReliableTopicTestException("test-exception in on_message")

                self._collector(event)

            def is_terminal(self):
                raise _ReliableTopicTestException("is_terminal failed")

        reliable_listener = TestReliableMessageListenerTerminal(collector)
        self.registration_id = self.reliable_topic.add_listener(reliable_listener)

        self.reliable_topic.publish('raise-exception')
        self.reliable_topic.publish('work-normally')
        time.sleep(0.5)
        self.assertEqual(len(collector.events), 0)

    def test_publish_many(self):
        collector = event_collector()
        reliable_listener = TestReliableMessageListener(collector)
        self.registration_id = self.reliable_topic.add_listener(reliable_listener)
        for i in range(10):
            self.reliable_topic.publish('message ' + str(i))

        def assert_event():
            self.assertEqual(len(collector.events), 10)

        self.assertTrueEventually(assert_event, 10)

    def test_message_field_set_correctly(self):
        collector = event_collector()
        reliable_listener = TestReliableMessageListener(collector)
        self.registration_id = self.reliable_topic.add_listener(reliable_listener)

        before_publish_time = current_time_in_millis()
        time.sleep(0.1)
        self.reliable_topic.publish('item-value')
        time.sleep(0.1)
        after_publish_time = current_time_in_millis()

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEqual(event.message, 'item-value')
            self.assertGreater(event.publish_time, before_publish_time)
            self.assertLess(event.publish_time, after_publish_time)

        self.assertTrueEventually(assert_event, 5)

    def test_always_start_after_tail(self):
        collector = event_collector()
        reliable_listener = TestReliableMessageListener(collector)
        self.reliable_topic.publish('1')
        self.reliable_topic.publish('2')
        self.reliable_topic.publish('3')

        self.registration_id = self.reliable_topic.add_listener(reliable_listener)

        self.reliable_topic.publish('4')
        self.reliable_topic.publish('5')
        self.reliable_topic.publish('6')

        def assert_event():
            self.assertEqual(len(collector.events), 3)
            self.assertEqual(collector.events[0].message, "4")
            self.assertEqual(collector.events[1].message, "5")
            self.assertEqual(collector.events[2].message, "6")

        self.assertTrueEventually(assert_event, 5)

    def generate_items(self, n):
        messages = []
        for i in range(n):
            msg = ReliableTopicMessage(
                publish_time=current_time_in_millis(),
                publisher_address="",
                payload=self.client.serialization_service.to_data(i+1)
            )
            messages.append(msg)

        return messages

    def test_discard(self):
        reliable_topic = self.client.get_reliable_topic("discard").blocking()
        items = self.generate_items(10)
        reliable_topic.ringbuffer.add_all(items, OVERFLOW_POLICY_FAIL)

        reliable_topic.publish(11)
        seq = reliable_topic.ringbuffer.tail_sequence().result()
        item = reliable_topic.ringbuffer.read_one(seq).result()
        num = self.client.serialization_service.to_object(item.payload)
        self.assertEqual(num, 10)

    def test_overwrite(self):
        reliable_topic = self.client.get_reliable_topic("overwrite").blocking()
        for i in range(10):
            reliable_topic.publish(i+1)

        reliable_topic.publish(11)
        seq = reliable_topic.ringbuffer.tail_sequence().result()
        item = reliable_topic.ringbuffer.read_one(seq).result()
        num = self.client.serialization_service.to_object(item.payload)
        self.assertEqual(num, 11)

    def test_error(self):
        reliable_topic = self.client.get_reliable_topic("error").blocking()
        for i in range(10):
            reliable_topic.publish(i+1)

        with self.assertRaises(TopicOverflowError):
            reliable_topic.publish(11)

    def test_blocking(self):
        reliable_topic = self.client.get_reliable_topic("blocking").blocking()
        for i in range(10):
            reliable_topic.publish(i+1)

        before = datetime.utcnow()
        reliable_topic.publish(11)
        time_diff = datetime.utcnow() - before

        seq = reliable_topic.ringbuffer.tail_sequence().result()
        item = reliable_topic.ringbuffer.read_one(seq).result()
        num = self.client.serialization_service.to_object(item.payload)
        self.assertEqual(num, 11)
        if time_diff.seconds <= 2:
            self.fail("expected at least 2 seconds delay got %s" % time_diff.seconds)

