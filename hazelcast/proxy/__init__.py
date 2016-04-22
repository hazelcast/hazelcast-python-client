from hazelcast.protocol.codec import client_create_proxy_codec, client_destroy_proxy_codec
from hazelcast.proxy.atomic_long import AtomicLong
from hazelcast.proxy.atomic_reference import AtomicReference
from hazelcast.proxy.count_down_latch import CountDownLatch
from hazelcast.proxy.executor import Executor
from hazelcast.proxy.id_generator import IdGenerator
from hazelcast.proxy.list import List
from hazelcast.proxy.lock import Lock
from hazelcast.proxy.map import create_map_proxy
from hazelcast.proxy.multi_map import MultiMap
from hazelcast.proxy.queue import Queue
from hazelcast.proxy.reliable_topic import ReliableTopic
from hazelcast.proxy.replicated_map import ReplicatedMap
from hazelcast.proxy.ringbuffer import Ringbuffer
from hazelcast.proxy.semaphore import Semaphore
from hazelcast.proxy.set import Set
from hazelcast.proxy.topic import Topic

ATOMIC_LONG_SERVICE = "hz:impl:atomicLongService"
ATOMIC_REFERENCE_SERVICE = "hz:impl:atomicReferenceService"
COUNT_DOWN_LATCH_SERVICE = "hz:impl:countDownLatchService"
ID_GENERATOR_SERVICE = "hz:impl:idGeneratorService"
EXECUTOR_SERVICE = "hz:impl:executorService"
LOCK_SERVICE = "hz:impl:lockService"
LIST_SERVICE = "hz:impl:listService"
MULTI_MAP_SERVICE = "hz:impl:multiMapService"
MAP_SERVICE = "hz:impl:mapService"
RELIABLE_TOPIC_SERVICE = "hz:impl:reliableTopicService"
REPLICATED_MAP_SERVICE = "hz:impl:replicatedMapService"
RINGBUFFER_SERIVCE = "hz:impl:ringbufferService"
SEMAPHORE_SERVICE = "hz:impl:semaphoreService"
SET_SERVICE = "hz:impl:setService"
QUEUE_SERVICE = "hz:impl:queueService"
TOPIC_SERVICE = "hz:impl:topicService"

ID_GENERATOR_ATOMIC_LONG_PREFIX = "hz:atomic:idGenerator:"

_proxy_init = {
    ATOMIC_LONG_SERVICE: AtomicLong,
    ATOMIC_REFERENCE_SERVICE: AtomicReference,
    COUNT_DOWN_LATCH_SERVICE: CountDownLatch,
    ID_GENERATOR_SERVICE: IdGenerator,
    EXECUTOR_SERVICE: Executor,
    LIST_SERVICE: List,
    LOCK_SERVICE: Lock,
    MAP_SERVICE: create_map_proxy,
    MULTI_MAP_SERVICE: MultiMap,
    QUEUE_SERVICE: Queue,
    RELIABLE_TOPIC_SERVICE: ReliableTopic,
    REPLICATED_MAP_SERVICE: ReplicatedMap,
    RINGBUFFER_SERIVCE: Ringbuffer,
    SEMAPHORE_SERVICE: Semaphore,
    SET_SERVICE: Set,
    TOPIC_SERVICE: Topic
}


class ProxyManager(object):
    def __init__(self, client):
        self._client = client
        self._proxies = {}

    def get_or_create(self, service_name, name, **kwargs):
        ns = (service_name, name)
        if ns in self._proxies:
            return self._proxies[ns]

        proxy = self.create_proxy(service_name, name, **kwargs)
        self._proxies[ns] = proxy
        return proxy

    def create_proxy(self, service_name, name, **kwargs):
        message = client_create_proxy_codec.encode_request(name=name, service_name=service_name,
                                                           target=self._find_next_proxy_address())
        self._client.invoker.invoke_on_random_target(message).result()
        return _proxy_init[service_name](client=self._client, service_name=service_name, name=name, **kwargs)

    def destroy_proxy(self, service_name, name):
        ns = (service_name, name)
        try:
            self._proxies.pop(ns)
            message = client_destroy_proxy_codec.encode_request(name=name, service_name=service_name)
            self._client.invoker.invoke_on_random_target(message).result()
            return True
        except KeyError:
            return False

    def _find_next_proxy_address(self):
        # TODO: filter out lite members
        return self._client.load_balancer.next_address()
