import asyncio
import typing

from hazelcast.internal.asyncio_proxy.executor import create_executor_proxy
from hazelcast.internal.asyncio_proxy.list import create_list_proxy
from hazelcast.internal.asyncio_proxy.multi_map import create_multi_map_proxy
from hazelcast.internal.asyncio_proxy.queue import create_queue_proxy
from hazelcast.internal.asyncio_proxy.set import create_set_proxy
from hazelcast.internal.asyncio_proxy.vector_collection import (
    create_vector_collection_proxy,
)
from hazelcast.protocol.codec import client_create_proxy_codec, client_destroy_proxy_codec
from hazelcast.internal.asyncio_invocation import Invocation
from hazelcast.internal.asyncio_proxy.base import Proxy
from hazelcast.internal.asyncio_proxy.map import create_map_proxy
from hazelcast.internal.asyncio_proxy.replicated_map import create_replicated_map_proxy
from hazelcast.internal.asyncio_proxy.ringbuffer import create_ringbuffer_proxy
from hazelcast.util import to_list

EXECUTOR_SERVICE = "hz:impl:executorService"
LIST_SERVICE = "hz:impl:listService"
MAP_SERVICE = "hz:impl:mapService"
MULTI_MAP_SERVICE = "hz:impl:multiMapService"
QUEUE_SERVICE = "hz:impl:queueService"
REPLICATED_MAP_SERVICE = "hz:impl:replicatedMapService"
RINGBUFFER_SERVICE = "hz:impl:ringbufferService"
SET_SERVICE = "hz:impl:setService"
VECTOR_SERVICE = "hz:service:vector"

_proxy_init: typing.Dict[
    str,
    typing.Callable[[str, str, typing.Any], typing.Coroutine[typing.Any, typing.Any, typing.Any]],
] = {
    EXECUTOR_SERVICE: create_executor_proxy,
    LIST_SERVICE: create_list_proxy,
    MAP_SERVICE: create_map_proxy,
    MULTI_MAP_SERVICE: create_multi_map_proxy,
    QUEUE_SERVICE: create_queue_proxy,
    REPLICATED_MAP_SERVICE: create_replicated_map_proxy,
    RINGBUFFER_SERVICE: create_ringbuffer_proxy,
    SET_SERVICE: create_set_proxy,
    VECTOR_SERVICE: create_vector_collection_proxy,
}


class ProxyManager:
    def __init__(self, context):
        self._context = context
        self._proxies = {}

    async def get_or_create(self, service_name, name, create_on_remote=True):
        ns = (service_name, name)
        proxy = self._proxies.get(ns)
        if proxy is not None:
            if isinstance(proxy, asyncio.Future):
                return await proxy
            return proxy

        # allocate the proxy slot, so a task that tries to access the same proxy knows it's being created
        fut = asyncio.get_running_loop().create_future()
        self._proxies[ns] = fut
        try:
            proxy = await self._create_proxy(service_name, name, create_on_remote)
        except BaseException as e:
            self._proxies.pop(ns, None)
            fut.set_exception(e)
            raise
        # replace the placeholder with the proxy
        self._proxies[ns] = proxy
        fut.set_result(proxy)
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
        return to_list(v for v in self._proxies.values() if not isinstance(v, asyncio.Future))
