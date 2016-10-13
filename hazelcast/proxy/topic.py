from hazelcast.protocol.codec import \
    topic_add_message_listener_codec, \
    topic_publish_codec, \
    topic_remove_message_listener_codec
from hazelcast.proxy.base import PartitionSpecificProxy, TopicMessage


class Topic(PartitionSpecificProxy):
    """
    Hazelcast provides distribution mechanism for publishing messages that are delivered to multiple subscribers, which
    is also known as a publish/subscribe (pub/sub) messaging model. Publish and subscriptions are cluster-wide. When a
    member subscribes for a topic, it is actually registering for messages published by any member in the cluster,
    including the new members joined after you added the listener.

    Messages are ordered, meaning that listeners(subscribers) will process the messages in the order they are actually
    published.

    """
    def add_listener(self, on_message=None):
        """
        Subscribes to this topic. When someone publishes a message on this topic, on_message() function is called if
        provided.

        :param on_message: (Function), function to be called when a message is published.
        :return: (str), a registration id which is used as a key to remove the listener.
        """
        request = topic_add_message_listener_codec.encode_request(self.name, False)

        def handle(item, publish_time, uuid):
            member = self._client.cluster.get_member_by_uuid(uuid)
            item_event = TopicMessage(self.name, item, publish_time, member, self._to_object)
            on_message(item_event)

        return self._start_listening(request,
                                     lambda m: topic_add_message_listener_codec.handle(m, handle),
                                     lambda r: topic_add_message_listener_codec.decode_response(r)['response'],
                                     self.partition_key)

    def publish(self, message):
        """
        Publishes the message to all subscribers of this topic

        :param message: (object), the message to be published.
        """
        message_data = self._to_data(message)
        self._encode_invoke(topic_publish_codec, message=message_data)

    def remove_listener(self, registration_id):
        """
        Stops receiving messages for the given message listener. If the given listener already removed, this method does
        nothing.

        :param registration_id: (str), registration id of the listener to be removed.
        :return: (bool), ``true`` if the listener is removed, ``false`` otherwise.
        """
        return self._stop_listening(registration_id,
                                    lambda i: topic_remove_message_listener_codec.encode_request(self.name, i))
