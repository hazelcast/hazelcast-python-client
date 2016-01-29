from hazelcast.protocol.codec import \
    topic_add_message_listener_codec, \
    topic_publish_codec, \
    topic_remove_message_listener_codec
from hazelcast.proxy.base import PartitionSpecificProxy, TopicMessage


class Topic(PartitionSpecificProxy):
    def add_listener(self, on_message=None):
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
        message_data = self._to_data(message)
        self._encode_invoke(topic_publish_codec, message=message_data)

    def remove_listener(self, registration_id):
        return self._stop_listening(registration_id,
                                    lambda i: topic_remove_message_listener_codec.encode_request(self.name, i))
