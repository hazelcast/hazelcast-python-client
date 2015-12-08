from hazelcast.protocol.codec import map_put_codec
from hazelcast.proxy.base import Proxy, check_not_none


class MapProxy(Proxy):
    def contains_key(self, key):
        '''

        :param key:
        :return:
        '''
        check_not_none("key can't be None")
        pass

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
        response = self.invoke_on_key(request, key_data).result()
        result_data = map_put_codec.decode_response(response)['response']
        return self.from_data(result_data)
        pass

    def get(self, key):
        '''
        :param key:
        :return:
        '''
        check_not_none("key can't be None")
