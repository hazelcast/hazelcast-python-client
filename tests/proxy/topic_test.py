from tests.base import SingleMemberTestCase
from tests.util import random_string, event_collector


class TopicTest(SingleMemberTestCase):
    def setUp(self):
        self.topic = self.client.get_topic(random_string()).blocking()

    def test_add_listener(self):
        collector = event_collector()
        reg_id = self.topic.add_listener(on_message=collector)
        self.topic.publish('item-value')

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEqual(event.message, 'item-value')
            self.assertGreater(event.publish_time, 0)

        self.assertTrueEventually(assert_event, 5)

    def test_remove_listener(self):
        collector = event_collector()
        reg_id = self.topic.add_listener(on_message=collector)
        self.topic.remove_listener(reg_id)
        self.topic.publish('item-value')

        def assert_event():
            self.assertEqual(len(collector.events), 0)
            if len(collector.events) > 0:
                event = collector.events[0]
                self.assertEqual(event.message, 'item-value')
                self.assertGreater(event.publish_time, 0)
        self.assertTrueEventually(assert_event, 5)

    def test_str(self):
        self.assertTrue(str(self.topic).startswith("Topic"))
