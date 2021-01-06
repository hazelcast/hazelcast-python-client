from hazelcast.protocol.codec import (
    topic_add_message_listener_codec,
    topic_publish_codec,
    topic_remove_message_listener_codec,
)
from hazelcast.proxy.base import PartitionSpecificProxy, TopicMessage


class Topic(PartitionSpecificProxy):
    """Hazelcast provides distribution mechanism for publishing messages that are delivered to multiple subscribers,
    which is also known as a publish/subscribe (pub/sub) messaging model.

    Publish and subscriptions are cluster-wide. When a member subscribes for a topic,
    it is actually registering for messages published by any member in the cluster,
    including the new members joined after you added the listener.

    Messages are ordered, meaning that listeners(subscribers) will process the messages in the order they are actually
    published.
    """

    def add_listener(self, on_message=None):
        """Subscribes to this topic.

        When someone publishes a message on this topic, ``on_message`` function is called if provided.

        Args:
            on_message (function): Function to be called when a message is published.

        Returns:
            hazelcast.future.Future[str]: A registration id which is used as a key to remove the listener.
        """
        codec = topic_add_message_listener_codec
        request = codec.encode_request(self.name, self._is_smart)

        def handle(item, publish_time, uuid):
            member = self._context.cluster_service.get_member(uuid)
            item_event = TopicMessage(
                self.name, item, publish_time / 1000.0, member, self._to_object
            )
            on_message(item_event)

        return self._register_listener(
            request,
            lambda r: codec.decode_response(r),
            lambda reg_id: topic_remove_message_listener_codec.encode_request(self.name, reg_id),
            lambda m: codec.handle(m, handle),
        )

    def publish(self, message):
        """Publishes the message to all subscribers of this topic

        Args:
            message: The message to be published.

        Returns:
            hazelcast.future.Future[None]:
        """
        message_data = self._to_data(message)
        request = topic_publish_codec.encode_request(self.name, message_data)
        return self._invoke(request)

    def remove_listener(self, registration_id):
        """Stops receiving messages for the given message listener.

        If the given listener already removed, this method does nothing.

        Args:
            registration_id (str): Registration id of the listener to be removed.

        Returns:
            hazelcast.future.Future[bool]: ``True`` if the listener is removed, ``False`` otherwise.
        """
        return self._deregister_listener(registration_id)
