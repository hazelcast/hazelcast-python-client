import typing

from hazelcast.internal.asyncio_proxy.vector_collection import (
    VectorCollection,
    create_vector_collection_proxy,
)
from hazelcast.protocol.codec import client_create_proxy_codec, client_destroy_proxy_codec
from hazelcast.internal.asyncio_invocation import Invocation
from hazelcast.internal.asyncio_proxy.base import Proxy
from hazelcast.internal.asyncio_proxy.map import create_map_proxy
from hazelcast.util import to_list

MAP_SERVICE = "hz:impl:mapService"
VECTOR_SERVICE = "hz:service:vector"

_proxy_init: typing.Dict[
    str,
    typing.Callable[[str, str, typing.Any], typing.Coroutine[typing.Any, typing.Any, typing.Any]],
] = {
    MAP_SERVICE: create_map_proxy,
    VECTOR_SERVICE: create_vector_collection_proxy,
}


class ProxyManager:
    def __init__(self, context):
        self._context = context
        self._proxies = {}

    async def get_or_create(self, service_name, name, create_on_remote=True):
        ns = (service_name, name)
        if ns in self._proxies:
            return self._proxies[ns]

        proxy = await self._create_proxy(service_name, name, create_on_remote)
        self._proxies[ns] = proxy
        return proxy

    async def _create_proxy(self, service_name, name, create_on_remote) -> Proxy:
        if create_on_remote:
            request = client_create_proxy_codec.encode_request(name, service_name)
            invocation = Invocation(request)
            invocation_service = self._context.invocation_service
            await invocation_service.ainvoke(invocation)

        return await _proxy_init[service_name](service_name, name, self._context)

    async def destroy_proxy(self, service_name, name, destroy_on_remote=True):
        ns = (service_name, name)
        try:
            self._proxies.pop(ns)
            if destroy_on_remote:
                request = client_destroy_proxy_codec.encode_request(name, service_name)
                invocation = Invocation(request)
                invocation_service = self._context.invocation_service
                await invocation_service.ainvoke(invocation)
            return True
        except KeyError:
            return False

    def get_distributed_objects(self):
        return to_list(self._proxies.values())
