from hazelcast.proxy.base import Proxy, check_not_none


class MapProxy(Proxy):
    def contains_key(self):
        pass

    def put(self, key, value, ttl=-1):
        check_not_none(key, "key can't be None")
        check_not_none(value, "key can't be None")

        key_data = self.to_data(key)
        value_data = self.to_data(value)

        # request = encode_request(...)
        # response = self.invoke_on_key(key, value)
        # return self.from_data(response.response)
        pass

    def get(self, key):
        pass
