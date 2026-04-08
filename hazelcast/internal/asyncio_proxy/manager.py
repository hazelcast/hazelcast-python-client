import asyncio
import typing

from hazelcast.internal.asyncio_proxy.list import create_list_proxy
from hazelcast.internal.asyncio_proxy.multi_map import create_multi_map_proxy
from hazelcast.internal.asyncio_proxy.vector_collection import (
    create_vector_collection_proxy,
)
from hazelcast.protocol.codec import client_create_proxy_codec, client_destroy_proxy_codec
from hazelcast.internal.asyncio_invocation import Invocation
from hazelcast.internal.asyncio_proxy.base import Proxy
from hazelcast.internal.asyncio_proxy.map import create_map_proxy
from hazelcast.internal.asyncio_proxy.reliable_topic import ReliableTopic
from hazelcast.internal.asyncio_proxy.replicated_map import create_replicated_map_proxy
from hazelcast.internal.asyncio_proxy.ringbuffer import create_ringbuffer_proxy
from hazelcast.proxy.reliable_topic import _RINGBUFFER_PREFIX
from hazelcast.util import to_list

LIST_SERVICE = "hz:impl:listService"
MAP_SERVICE = "hz:impl:mapService"
MULTI_MAP_SERVICE = "hz:impl:multiMapService"
RELIABLE_TOPIC_SERVICE = "hz:impl:reliableTopicService"
REPLICATED_MAP_SERVICE = "hz:impl:replicatedMapService"
RINGBUFFER_SERVICE = "hz:impl:ringbufferService"
VECTOR_SERVICE = "hz:service:vector"


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


async def create_reliable_topic_proxy(service_name, name, context):
    ringbuffer = await context.proxy_manager.get_or_create(
        RINGBUFFER_SERVICE, _RINGBUFFER_PREFIX + name, create_on_remote=False
    )
    return ReliableTopic(service_name, name, context, ringbuffer)


_proxy_init: typing.Dict[
    str,
    typing.Callable[[str, str, typing.Any], typing.Coroutine[typing.Any, typing.Any, typing.Any]],
] = {
    LIST_SERVICE: create_list_proxy,
    MAP_SERVICE: create_map_proxy,
    MULTI_MAP_SERVICE: create_multi_map_proxy,
    RELIABLE_TOPIC_SERVICE: create_reliable_topic_proxy,
    REPLICATED_MAP_SERVICE: create_replicated_map_proxy,
    RINGBUFFER_SERVICE: create_ringbuffer_proxy,
    VECTOR_SERVICE: create_vector_collection_proxy,
}
