import typing

from hazelcast.future import Future
from hazelcast.protocol.codec import (
    topic_add_message_listener_codec,
    topic_publish_codec,
    topic_publish_all_codec,
    topic_remove_message_listener_codec,
)
from hazelcast.proxy.base import PartitionSpecificProxy, TopicMessage
from hazelcast.serialization.compact import SchemaNotReplicatedError
from hazelcast.types import MessageType
from hazelcast.util import check_not_none


class Topic(PartitionSpecificProxy["BlockingTopic"], typing.Generic[MessageType]):
    """Hazelcast provides distribution mechanism for publishing messages that
    are delivered to multiple subscribers, which is also known as a
    publish/subscribe (pub/sub) messaging model.

    Publish and subscriptions are cluster-wide. When a member subscribes for
    a topic, it is actually registering for messages published by any member
    in the cluster, including the new members joined after you added the
    listener.

    Messages are ordered, meaning that listeners(subscribers) will process the
    messages in the order they are actually published.
    """

    def add_listener(
        self, on_message: typing.Callable[[TopicMessage[MessageType]], None] = None
    ) -> Future[str]:
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

        return self._register_listener(
            request,
            lambda r: codec.decode_response(r),
            lambda reg_id: topic_remove_message_listener_codec.encode_request(self.name, reg_id),
            lambda m: codec.handle(m, handle),
        )

    def publish(self, message: MessageType) -> Future[None]:
        """Publishes the message to all subscribers of this topic.

        Args:
            message: The message to be published.
        """
        try:
            message_data = self._to_data(message)
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.publish, message)

        request = topic_publish_codec.encode_request(self.name, message_data)
        return self._invoke(request)

    def publish_all(self, messages: typing.Sequence[MessageType]) -> Future[None]:
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
            return self._send_schema_and_retry(e, self.publish_all, messages)

        request = topic_publish_all_codec.encode_request(self.name, topic_messages)
        return self._invoke(request)

    def remove_listener(self, registration_id: str) -> Future[bool]:
        """Stops receiving messages for the given message listener.

        If the given listener already removed, this method does nothing.

        Args:
            registration_id: Registration id of the listener to be removed.

        Returns:
            ``True`` if the listener is removed, ``False`` otherwise.
        """
        return self._deregister_listener(registration_id)

    def blocking(self) -> "BlockingTopic[MessageType]":
        return BlockingTopic(self)


class BlockingTopic(Topic[MessageType]):
    __slots__ = ("_wrapped", "name", "service_name")

    def __init__(self, wrapped: Topic[MessageType]):
        self.name = wrapped.name
        self.service_name = wrapped.service_name
        self._wrapped = wrapped

    def add_listener(  # type: ignore[override]
        self,
        on_message: typing.Callable[[TopicMessage[MessageType]], None] = None,
    ) -> str:
        return self._wrapped.add_listener(on_message).result()

    def publish(  # type: ignore[override]
        self,
        message: MessageType,
    ) -> None:
        return self._wrapped.publish(message).result()

    def publish_all(  # type: ignore[override]
        self,
        messages: typing.Sequence[MessageType],
    ) -> None:
        return self._wrapped.publish_all(messages).result()

    def remove_listener(  # type: ignore[override]
        self,
        registration_id: str,
    ) -> bool:
        return self._wrapped.remove_listener(registration_id).result()

    def destroy(self) -> bool:
        return self._wrapped.destroy()

    def blocking(self) -> "BlockingTopic[MessageType]":
        return self

    def __repr__(self) -> str:
        return self._wrapped.__repr__()
