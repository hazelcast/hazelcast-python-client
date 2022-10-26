from tests.base import SingleMemberTestCase
from tests.util import random_string, event_collector, skip_if_client_version_older_than


class TopicTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        return config

    def setUp(self):
        self.topic = self.client.get_topic(random_string()).blocking()

    def tearDown(self):
        self.topic.destroy()

    def test_add_listener(self):
        collector = event_collector()
        self.topic.add_listener(on_message=collector)
        self.topic.publish("item-value")

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEqual(event.message, "item-value")
            self.assertGreater(event.publish_time, 0)

        self.assertTrueEventually(assert_event, 5)

    def test_remove_listener(self):
        collector = event_collector()
        reg_id = self.topic.add_listener(on_message=collector)
        self.topic.remove_listener(reg_id)
        self.topic.publish("item-value")

        def assert_event():
            self.assertEqual(len(collector.events), 0)
            if len(collector.events) > 0:
                event = collector.events[0]
                self.assertEqual(event.message, "item-value")
                self.assertGreater(event.publish_time, 0)

        self.assertTrueEventually(assert_event, 5)

    def test_str(self):
        self.assertTrue(str(self.topic).startswith("Topic"))

    def test_publish_all(self):
        skip_if_client_version_older_than(self, "5.2")
        collector = event_collector()
        self.topic.add_listener(on_message=collector)

        messages = ["message1", "message2", "message3"]
        self.topic.publish_all(messages)

        def assert_event():
            self.assertEqual(len(collector.events), 3)

        self.assertTrueEventually(assert_event, 5)

    def test_publish_all_none_messages(self):
        skip_if_client_version_older_than(self, "5.2")
        with self.assertRaises(AssertionError):
            self.topic.publish_all(None)

    def test_publish_all_none_message(self):
        skip_if_client_version_older_than(self, "5.2")
        messages = ["message1", None, "message3"]
        with self.assertRaises(AssertionError):
            self.topic.publish_all(messages)
