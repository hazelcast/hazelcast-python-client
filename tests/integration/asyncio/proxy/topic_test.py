from tests.integration.asyncio.base import SingleMemberTestCase
from tests.util import (
    random_string,
    event_collector,
    skip_if_client_version_older_than,
    skip_if_server_version_older_than,
)


class TopicTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        return config

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.topic = await self.client.get_topic(random_string())

    async def asyncTearDown(self):
        await self.topic.destroy()
        await super().asyncTearDown()

    async def test_add_listener(self):
        collector = event_collector()
        await self.topic.add_listener(on_message=collector)
        await self.topic.publish("item-value")

        def assert_event():
            self.assertEqual(len(collector.events), 1)
            event = collector.events[0]
            self.assertEqual(event.message, "item-value")
            self.assertGreater(event.publish_time, 0)

        await self.assertTrueEventually(assert_event, 5)

    async def test_remove_listener(self):
        collector = event_collector()
        reg_id = await self.topic.add_listener(on_message=collector)
        await self.topic.remove_listener(reg_id)
        await self.topic.publish("item-value")

        def assert_event():
            self.assertEqual(len(collector.events), 0)
            if len(collector.events) > 0:
                event = collector.events[0]
                self.assertEqual(event.message, "item-value")
                self.assertGreater(event.publish_time, 0)

        await self.assertTrueEventually(assert_event, 5)

    async def test_str(self):
        self.assertTrue(str(self.topic).startswith("Topic"))

    async def test_publish_all(self):
        skip_if_client_version_older_than(self, "5.2")
        skip_if_server_version_older_than(self, self.client, "4.1")

        collector = event_collector()
        await self.topic.add_listener(on_message=collector)

        messages = ["message1", "message2", "message3"]
        await self.topic.publish_all(messages)

        def assert_event():
            self.assertEqual(len(collector.events), 3)

        await self.assertTrueEventually(assert_event, 5)

    async def test_publish_all_none_messages(self):
        skip_if_client_version_older_than(self, "5.2")
        skip_if_server_version_older_than(self, self.client, "4.1")

        with self.assertRaises(AssertionError):
            await self.topic.publish_all(None)

    async def test_publish_all_none_message(self):
        skip_if_client_version_older_than(self, "5.2")
        skip_if_server_version_older_than(self, self.client, "4.1")

        messages = ["message1", None, "message3"]
        with self.assertRaises(AssertionError):
            await self.topic.publish_all(messages)
