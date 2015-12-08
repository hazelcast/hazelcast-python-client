from hazelcast.protocol.codec import map_contains_key_codec, map_get_codec, map_put_codec, map_size_codec
from hazelcast.proxy.base import Proxy, check_not_none


class MapProxy(Proxy):
    def contains_key(self, key):
        '''

        :param key:
        :return:
        '''
        check_not_none("key can't be None")
        key_data = self.to_data(key)

        request = map_contains_key_codec.encode_request(self.name, key_data, thread_id=self.thread_id())
        response = self.invoke_on_key(request, key_data)
        return map_get_codec.decode_response(response)['response']

    def put(self, key, value, ttl=-1):
        '''
        :param key:
        :param value:
        :param ttl:
        :return:
        '''
        check_not_none(key, "key can't be None")
        check_not_none(value, "value can't be None")

        key_data = self.to_data(key)
        value_data = self.to_data(value)

        request = map_put_codec.encode_request(self.name, key_data, value_data, thread_id=self.thread_id(), ttl=ttl)
        response = self.invoke_on_key(request, key_data)
        result_data = map_put_codec.decode_response(response)['response']
        return self.to_object(result_data)

    def get(self, key):
        '''
        :param key:
        :return:
        '''
        check_not_none("key can't be None")

        key_data = self.to_data(key)
        request = map_get_codec.encode_request(self.name, key_data, thread_id=self.thread_id())
        response = self.invoke_on_key(request, key_data)
        result_data = map_get_codec.decode_response(response)['response']
        return self.to_object(result_data)

    def size(self):
        request = map_size_codec.encode_request(self.name)
        response = self.invoke(request)
        return map_size_codec.decode_response(response)["response"]
