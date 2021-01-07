from hazelcast.invocation import Invocation
from hazelcast.protocol.codec import client_create_proxy_codec, client_destroy_proxy_codec
from hazelcast.proxy.executor import Executor
from hazelcast.proxy.list import List
from hazelcast.proxy.map import create_map_proxy
from hazelcast.proxy.multi_map import MultiMap
from hazelcast.proxy.queue import Queue
from hazelcast.proxy.reliable_topic import ReliableTopic
from hazelcast.proxy.replicated_map import ReplicatedMap
from hazelcast.proxy.ringbuffer import Ringbuffer
from hazelcast.proxy.set import Set
from hazelcast.proxy.topic import Topic
from hazelcast.proxy.pn_counter import PNCounter
from hazelcast.proxy.flake_id_generator import FlakeIdGenerator
from hazelcast.util import to_list

EXECUTOR_SERVICE = "hz:impl:executorService"
LIST_SERVICE = "hz:impl:listService"
MULTI_MAP_SERVICE = "hz:impl:multiMapService"
MAP_SERVICE = "hz:impl:mapService"
RELIABLE_TOPIC_SERVICE = "hz:impl:reliableTopicService"
REPLICATED_MAP_SERVICE = "hz:impl:replicatedMapService"
RINGBUFFER_SERVICE = "hz:impl:ringbufferService"
SET_SERVICE = "hz:impl:setService"
QUEUE_SERVICE = "hz:impl:queueService"
TOPIC_SERVICE = "hz:impl:topicService"
PN_COUNTER_SERVICE = "hz:impl:PNCounterService"
FLAKE_ID_GENERATOR_SERVICE = "hz:impl:flakeIdGeneratorService"

_proxy_init = {
    EXECUTOR_SERVICE: Executor,
    LIST_SERVICE: List,
    MAP_SERVICE: create_map_proxy,
    MULTI_MAP_SERVICE: MultiMap,
    QUEUE_SERVICE: Queue,
    RELIABLE_TOPIC_SERVICE: ReliableTopic,
    REPLICATED_MAP_SERVICE: ReplicatedMap,
    RINGBUFFER_SERVICE: Ringbuffer,
    SET_SERVICE: Set,
    TOPIC_SERVICE: Topic,
    PN_COUNTER_SERVICE: PNCounter,
    FLAKE_ID_GENERATOR_SERVICE: FlakeIdGenerator,
}


class ProxyManager(object):
    def __init__(self, context):
        self._context = context
        self._proxies = {}

    def get_or_create(self, service_name, name, create_on_remote=True):
        ns = (service_name, name)
        if ns in self._proxies:
            return self._proxies[ns]

        proxy = self._create_proxy(service_name, name, create_on_remote)
        self._proxies[ns] = proxy
        return proxy

    def _create_proxy(self, service_name, name, create_on_remote):
        if create_on_remote:
            request = client_create_proxy_codec.encode_request(name, service_name)
            invocation = Invocation(request)
            invocation_service = self._context.invocation_service
            invocation_service.invoke(invocation)
            invocation.future.result()

        return _proxy_init[service_name](service_name, name, self._context)

    def destroy_proxy(self, service_name, name, destroy_on_remote=True):
        ns = (service_name, name)
        try:
            self._proxies.pop(ns)
            if destroy_on_remote:
                request = client_destroy_proxy_codec.encode_request(name, service_name)
                invocation = Invocation(request)
                invocation_service = self._context.invocation_service
                invocation_service.invoke(invocation)
                invocation.future.result()
            return True
        except KeyError:
            return False

    def get_distributed_objects(self):
        return to_list(self._proxies.values())
