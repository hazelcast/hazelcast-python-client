from hazelcast.proxy.map import MapProxy

MAP_SERVICE = "hz:impl:mapService"

_proxy_init = {MAP_SERVICE: MapProxy}


class ProxyManager(object):
    def __init__(self, client):
        self._client = client
        self._proxies = {}

    def get_or_create(self, service, name):
        if name in self._proxies:
            return self._proxies[name]

        proxy = self.create_proxy(service, name)
        self._proxies[name] = proxy
        return proxy

    def create_proxy(self, service, name):
        message = ""  # create proxy message
        # self._client.invoker.invoke_on_random_target(message).result()
        return _proxy_init[service](client=self._client, service_name=service, name=name)
