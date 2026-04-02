import typing

from hazelcast.protocol.codec import (
    topic_add_message_listener_codec,
    topic_publish_codec,
    topic_publish_all_codec,
    topic_remove_message_listener_codec,
)
from hazelcast.internal.asyncio_proxy.base import PartitionSpecificProxy
from hazelcast.proxy.base import TopicMessage
from hazelcast.serialization.compact import SchemaNotReplicatedError
from hazelcast.types import MessageType
from hazelcast.util import check_not_none


class Topic(PartitionSpecificProxy, typing.Generic[MessageType]):
    """Hazelcast provides distribution mechanism for publishing messages that
    are delivered to multiple subscribers, which is also known as a
    publish/subscribe (pub/sub) messaging model.

    Publish and subscriptions are cluster-wide. When a member subscribes to
    a topic, it is actually registering for messages published by any member
    in the cluster, including the new members joined after you added the
    listener.

    Messages are ordered, meaning that listeners(subscribers) will process the
    messages in the order they are actually published.

    Example:
        >>> my_topic = await client.get_topic("my_topic")
        >>> await my_topic.publish("hello")

    Warning:
        Asyncio client topic proxy is not thread-safe, do not access it from other threads.
    """

    async def add_listener(
        self, on_message: typing.Callable[[TopicMessage[MessageType]], None] = None
    ) -> str:
        """Subscribes to this topic.

        When someone publishes a message on this topic, ``on_message`` function
        is called if provided.

        Args:
            on_message: Function to be called when a message is published.

        Returns:
            A registration id which is used as a key to remove the listener.
        """
        codec = topic_add_message_listener_codec
        request = codec.encode_request(self.name, self._is_smart)

        def handle(item_data, publish_time, uuid):
            member = self._context.cluster_service.get_member(uuid)
            item_event = TopicMessage(
                self.name, self._to_object(item_data), publish_time / 1000.0, member
            )
            on_message(item_event)

        return await self._register_listener(
            request,
            lambda r: codec.decode_response(r),
            lambda reg_id: topic_remove_message_listener_codec.encode_request(self.name, reg_id),
            lambda m: codec.handle(m, handle),
        )

    async def publish(self, message: MessageType) -> None:
        """Publishes the message to all subscribers of this topic.

        Args:
            message: The message to be published.
        """
        try:
            message_data = self._to_data(message)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.publish, message)

        request = topic_publish_codec.encode_request(self.name, message_data)
        return await self._invoke(request)

    async def publish_all(self, messages: typing.Sequence[MessageType]) -> None:
        """Publishes the messages to all subscribers of this topic.

        Args:
            messages: The messages to be published.
        """
        check_not_none(messages, "Messages cannot be None")
        try:
            topic_messages = []
            for m in messages:
                check_not_none(m, "Message cannot be None")
                data = self._to_data(m)
                topic_messages.append(data)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.publish_all, messages)

        request = topic_publish_all_codec.encode_request(self.name, topic_messages)
        return await self._invoke(request)

    async def remove_listener(self, registration_id: str) -> bool:
        """Stops receiving messages for the given message listener.

        If the given listener already removed, this method does nothing.

        Args:
            registration_id: Registration id of the listener to be removed.

        Returns:
            ``True`` if the listener is removed, ``False`` otherwise.
        """
        return await self._deregister_listener(registration_id)


async def create_topic_proxy(service_name, name, context):
    return Topic(service_name, name, context)
