from hazelcast.protocol.codec import client_create_proxy_codec
from hazelcast.proxy.map import MapProxy
from hazelcast.proxy.queue import QueueProxy

MAP_SERVICE = "hz:impl:mapService"
QUEUE_SERVICE = "hz:impl:queueService"

_proxy_init = {MAP_SERVICE: MapProxy, QUEUE_SERVICE: QueueProxy}


class ProxyManager(object):
    def __init__(self, client):
        self._client = client
        self._proxies = {}

    def get_or_create(self, service_name, name):
        if name in self._proxies:
            return self._proxies[name]

        proxy = self.create_proxy(service_name, name)
        self._proxies[name] = proxy
        return proxy

    def create_proxy(self, service_name, name):
        message = client_create_proxy_codec.encode_request(name=name, service_name=service_name,
                                                           target=self._find_next_proxy_address())
        self._client.invoker.invoke_on_random_target(message).result()
        return _proxy_init[service_name](client=self._client, service_name=service_name, name=name)

    def _find_next_proxy_address(self):
        # TODO: filter out lite members
        return self._client.load_balancer.next_address()
