from hazelcast.core import DistributedObjectEvent
from hazelcast.protocol.codec import client_create_proxy_codec, client_destroy_proxy_codec, \
    client_add_distributed_object_listener_codec, client_remove_distributed_object_listener_codec
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
from hazelcast.proxy.pn_counter import PNCounter
from hazelcast.proxy.flake_id_generator import FlakeIdGenerator
from hazelcast.util import to_list

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
RINGBUFFER_SERVICE = "hz:impl:ringbufferService"
SEMAPHORE_SERVICE = "hz:impl:semaphoreService"
SET_SERVICE = "hz:impl:setService"
QUEUE_SERVICE = "hz:impl:queueService"
TOPIC_SERVICE = "hz:impl:topicService"
PN_COUNTER_SERVICE = "hz:impl:PNCounterService"
FLAKE_ID_GENERATOR_SERVICE = "hz:impl:flakeIdGeneratorService"

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
    RINGBUFFER_SERVICE: Ringbuffer,
    SEMAPHORE_SERVICE: Semaphore,
    SET_SERVICE: Set,
    TOPIC_SERVICE: Topic,
    PN_COUNTER_SERVICE: PNCounter,
    FLAKE_ID_GENERATOR_SERVICE: FlakeIdGenerator
}


class ProxyManager(object):
    def __init__(self, client):
        self._client = client
        self._proxies = {}

    def get_or_create(self, service_name, name, create_on_remote=True, **kwargs):
        ns = (service_name, name)
        if ns in self._proxies:
            return self._proxies[ns]

        proxy = self.create_proxy(service_name, name, create_on_remote, **kwargs)
        self._proxies[ns] = proxy
        return proxy

    def create_proxy(self, service_name, name, create_on_remote, **kwargs):
        if create_on_remote:
            message = client_create_proxy_codec.encode_request(name=name, service_name=service_name,
                                                               target=self._find_next_proxy_address())
            self._client.invoker.invoke_on_random_target(message).result()

        return _proxy_init[service_name](client=self._client, service_name=service_name, name=name, **kwargs)

    def destroy_proxy(self, service_name, name, destroy_on_remote=True):
        ns = (service_name, name)
        try:
            self._proxies.pop(ns)
            if destroy_on_remote:
                message = client_destroy_proxy_codec.encode_request(name=name, service_name=service_name)
                self._client.invoker.invoke_on_random_target(message).result()
            return True
        except KeyError:
            return False

    def get_distributed_objects(self):
        return to_list(self._proxies.values())

    def add_distributed_object_listener(self, listener_func):
        is_smart = self._client.config.network_config.smart_routing
        request = client_add_distributed_object_listener_codec.encode_request(is_smart)

        def handle_distributed_object_event(**kwargs):
            event = DistributedObjectEvent(**kwargs)
            listener_func(event)

        def event_handler(client_message):
            return client_add_distributed_object_listener_codec.handle(client_message, handle_distributed_object_event)

        def decode_add_listener(response):
            return client_add_distributed_object_listener_codec.decode_response(response)["response"]

        def encode_remove_listener(registration_id):
            return client_remove_distributed_object_listener_codec.encode_request(registration_id)

        return self._client.listener.register_listener(request, decode_add_listener,
                                                       encode_remove_listener, event_handler)

    def remove_distributed_object_listener(self, registration_id):
        return self._client.listener.deregister_listener(registration_id)

    def _find_next_proxy_address(self):
        # TODO: filter out lite members
        return self._client.load_balancer.next_address()
