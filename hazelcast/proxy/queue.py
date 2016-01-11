from hazelcast.protocol.codec import queue_offer_codec, queue_poll_codec, queue_peek_codec, \
    queue_remaining_capacity_codec, queue_contains_codec, queue_remove_codec
from hazelcast.proxy.base import Proxy
from hazelcast.util import check_not_none


class Queue(Proxy):
    def __init__(self, client, service_name, name):
        Proxy.__init__(self, client, service_name, name)
        # the line below could be wrong since it uses python serialization of the string
        # and might not give correct partitionId where queue lives on
        self._partition_id = self._client.partition_service.get_partition_id(self._to_data(name))

    def add_all(self, list):
        raise NotImplementedError

    def add_listener(self, added=None, removed=None):
        raise NotImplementedError

    def clear(self, item):
        raise NotImplementedError

    def contains(self, item):
        check_not_none(item, "item can't be None")
        item_data = self._to_data(item)
        request = queue_contains_codec.encode_request(self.name, item_data)
        response = self._invoke_on_partition(request, self._partition_id)
        return queue_contains_codec.decode_response(response)['response']

    def contains_all(self, item):
        raise NotImplementedError

    def drain_to(self, list, max_elements=None):
        raise NotImplementedError

    def get_all(self):
        raise NotImplementedError

    def is_empty(self):
        raise NotImplementedError

    def offer(self, item):
        check_not_none(item, "item can't be None")
        item_data = self._to_data(item)
        request = queue_offer_codec.encode_request(self.name, item_data, 0)
        response = self._invoke_on_partition(request, self._partition_id)
        return queue_offer_codec.decode_response(response)['response']

    def peek(self):
        request = queue_peek_codec.encode_request(self.name)
        response = self._invoke_on_partition(request, self._partition_id)
        result_data = queue_peek_codec.decode_response(response)['response']
        return self._to_object(result_data)

    def poll(self):
        request = queue_poll_codec.encode_request(self.name, 0)
        response = self._invoke_on_partition(request, self._partition_id)
        result_data = queue_poll_codec.decode_response(response)['response']
        return self._to_object(result_data)

    def put(self, item):
        raise NotImplementedError

    def remaining_capacity(self):
        request = queue_remaining_capacity_codec.encode_request(self.name)
        response = self._invoke_on_partition(request, self._partition_id)
        return queue_remaining_capacity_codec.decode_response(response)['response']

    def remove(self, item):
        check_not_none(item, "item can't be None")
        item_data = self._to_data(item)
        request = queue_remove_codec.encode_request(self.name, item_data)
        response = self._invoke_on_partition(request, self._partition_id)
        return queue_remove_codec.decode_response(response)['response']

    def remove_all(self, list):
        raise NotImplementedError

    def remove_listener(self, registration_id):
        raise NotImplementedError

    def retain_all(self, list):
        raise NotImplementedError

    def size(self):
        raise NotImplementedError

    def take(self):
        raise NotImplementedError

    def __str__(self):
        return "Queue(name=%s)" % self.name
