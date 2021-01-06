import time
from threading import RLock, Lock

from hazelcast import six
from hazelcast.errors import (
    SessionExpiredError,
    CPGroupDestroyedError,
    HazelcastClientNotActiveError,
)
from hazelcast.future import ImmediateExceptionFuture, ImmediateFuture, combine_futures
from hazelcast.invocation import Invocation
from hazelcast.protocol.codec import (
    cp_group_create_cp_group_codec,
    cp_session_heartbeat_session_codec,
    cp_session_create_session_codec,
    cp_session_close_session_codec,
    cp_session_generate_thread_id_codec,
    semaphore_get_semaphore_type_codec,
)
from hazelcast.proxy.cp.atomic_long import AtomicLong
from hazelcast.proxy.cp.atomic_reference import AtomicReference
from hazelcast.proxy.cp.count_down_latch import CountDownLatch
from hazelcast.proxy.cp.fenced_lock import FencedLock
from hazelcast.proxy.cp.semaphore import SessionAwareSemaphore, SessionlessSemaphore
from hazelcast.util import check_true, AtomicInteger, thread_id


class CPSubsystem(object):
    """CP Subsystem is a component of Hazelcast that builds a strongly consistent
    layer for a set of distributed data structures.

    Its APIs can be used for implementing distributed coordination use cases,
    such as leader election, distributed locking, synchronization, and metadata
    management.

    Its data structures are CP with respect to the CAP principle, i.e., they
    always maintain linearizability and prefer consistency over availability
    during network partitions. Besides network partitions, CP Subsystem
    withstands server and client failures.

    Data structures in CP Subsystem run in CP groups. Each CP group elects
    its own Raft leader and runs the Raft consensus algorithm independently.

    The CP data structures differ from the other Hazelcast data structures
    in two aspects. First, an internal commit is performed on the METADATA CP
    group every time you fetch a proxy from this interface. Hence, callers
    should cache returned proxy objects. Second, if you call ``destroy()``
    on a CP data structure proxy, that data structure is terminated on the
    underlying CP group and cannot be reinitialized until the CP group is
    force-destroyed. For this reason, please make sure that you are completely
    done with a CP data structure before destroying its proxy.
    """

    def __init__(self, context):
        self._proxy_manager = CPProxyManager(context)

    def get_atomic_long(self, name):
        """Returns the distributed AtomicLong instance with given name.

        The instance is created on CP Subsystem.

        If no group name is given within the ``name`` argument, then the
        AtomicLong instance will be created on the default CP group.
        If a group name is given, like ``.get_atomic_long("myLong@group1")``,
        the given group will be initialized first, if not initialized
        already, and then the instance will be created on this group.

        Args:
            name (str): Name of the AtomicLong.

        Returns:
            hazelcast.proxy.cp.atomic_long.AtomicLong: The AtomicLong proxy
            for the given name.
        """
        return self._proxy_manager.get_or_create(ATOMIC_LONG_SERVICE, name)

    def get_atomic_reference(self, name):
        """Returns the distributed AtomicReference instance with given name.

        The instance is created on CP Subsystem.

        If no group name is given within the ``name`` argument, then the
        AtomicLong instance will be created on the DEFAULT CP group.
        If a group name is given, like ``.get_atomic_reference("myRef@group1")``,
        the given group will be initialized first, if not initialized
        already, and then the instance will be created on this group.

        Args:
            name (str): Name of the AtomicReference.

        Returns:
            hazelcast.proxy.cp.atomic_reference.AtomicReference: The AtomicReference
            proxy for the given name.
        """
        return self._proxy_manager.get_or_create(ATOMIC_REFERENCE_SERVICE, name)

    def get_count_down_latch(self, name):
        """Returns the distributed CountDownLatch instance with given name.

        The instance is created on CP Subsystem.

        If no group name is given within the ``name`` argument, then the
        CountDownLatch instance will be created on the DEFAULT CP group.
        If a group name is given, like ``.get_count_down_latch("myLatch@group1")``,
        the given group will be initialized first, if not initialized
        already, and then the instance will be created on this group.

        Args:
            name (str): Name of the CountDownLatch.

        Returns:
            hazelcast.proxy.cp.count_down_latch.CountDownLatch: The CountDownLatch
            proxy for the given name.
        """
        return self._proxy_manager.get_or_create(COUNT_DOWN_LATCH_SERVICE, name)

    def get_lock(self, name):
        """Returns the distributed FencedLock instance instance with given name.

        The instance is created on CP Subsystem.

        If no group name is given within the ``name`` argument, then the
        FencedLock instance will be created on the DEFAULT CP group.
        If a group name is given, like ``.get_lock("myLock@group1")``,
        the given group will be initialized first, if not initialized
        already, and then the instance will be created on this group.

        Args:
            name (str): Name of the FencedLock

        Returns:
            hazelcast.proxy.cp.fenced_lock.FencedLock: The FencedLock proxy
            for the given name.
        """
        return self._proxy_manager.get_or_create(LOCK_SERVICE, name)

    def get_semaphore(self, name):
        """Returns the distributed Semaphore instance instance with given name.

        The instance is created on CP Subsystem.

        If no group name is given within the ``name`` argument, then the
        Semaphore instance will be created on the DEFAULT CP group.
        If a group name is given, like ``.get_semaphore("mySemaphore@group1")``,
        the given group will be initialized first, if not initialized
        already, and then the instance will be created on this group.

        Args:
            name (str): Name of the Semaphore

        Returns:
            hazelcast.proxy.cp.semaphore.Semaphore: The Semaphore proxy for the given name.
        """
        return self._proxy_manager.get_or_create(SEMAPHORE_SERVICE, name)


_DEFAULT_GROUP_NAME = "default"


def _without_default_group_name(name):
    name = name.strip()
    idx = name.find("@")
    if idx == -1:
        return name

    check_true(name.find("@", idx + 1) == -1, "Custom group name must be specified at most once")
    group_name = name[idx + 1 :].strip()
    if group_name == _DEFAULT_GROUP_NAME:
        return name[:idx]
    return name


def _get_object_name_for_proxy(name):
    idx = name.find("@")
    if idx == -1:
        return name

    group_name = name[idx + 1 :].strip()
    check_true(len(group_name) > 0, "Custom CP group name cannot be empty string")
    object_name = name[:idx].strip()
    check_true(len(object_name) > 0, "Object name cannot be empty string")
    return object_name


ATOMIC_LONG_SERVICE = "hz:raft:atomicLongService"
ATOMIC_REFERENCE_SERVICE = "hz:raft:atomicRefService"
COUNT_DOWN_LATCH_SERVICE = "hz:raft:countDownLatchService"
LOCK_SERVICE = "hz:raft:lockService"
SEMAPHORE_SERVICE = "hz:raft:semaphoreService"


class CPProxyManager(object):
    def __init__(self, context):
        self._context = context
        self._lock_proxies = dict()  # proxy_name to FencedLock
        self._mux = Lock()  # Guards the _lock_proxies

    def get_or_create(self, service_name, proxy_name):
        proxy_name = _without_default_group_name(proxy_name)
        object_name = _get_object_name_for_proxy(proxy_name)

        group_id = self._get_group_id(proxy_name)
        if service_name == ATOMIC_LONG_SERVICE:
            return AtomicLong(self._context, group_id, service_name, proxy_name, object_name)
        elif service_name == ATOMIC_REFERENCE_SERVICE:
            return AtomicReference(self._context, group_id, service_name, proxy_name, object_name)
        elif service_name == COUNT_DOWN_LATCH_SERVICE:
            return CountDownLatch(self._context, group_id, service_name, proxy_name, object_name)
        elif service_name == LOCK_SERVICE:
            return self._create_fenced_lock(group_id, proxy_name, object_name)
        elif service_name == SEMAPHORE_SERVICE:
            return self._create_semaphore(group_id, proxy_name, object_name)
        else:
            raise ValueError("Unknown service name: %s" % service_name)

    def _create_fenced_lock(self, group_id, proxy_name, object_name):
        with self._mux:
            proxy = self._lock_proxies.get(proxy_name, None)
            if proxy:
                if proxy.get_group_id() != group_id:
                    self._lock_proxies.pop(proxy_name, None)
                else:
                    return proxy

            proxy = FencedLock(self._context, group_id, LOCK_SERVICE, proxy_name, object_name)
            self._lock_proxies[proxy_name] = proxy
            return proxy

    def _create_semaphore(self, group_id, proxy_name, object_name):
        codec = semaphore_get_semaphore_type_codec
        request = codec.encode_request(proxy_name)
        invocation = Invocation(request, response_handler=codec.decode_response)
        invocation_service = self._context.invocation_service
        invocation_service.invoke(invocation)
        jdk_compatible = invocation.future.result()
        if jdk_compatible:
            return SessionlessSemaphore(
                self._context, group_id, SEMAPHORE_SERVICE, proxy_name, object_name
            )
        else:
            return SessionAwareSemaphore(
                self._context, group_id, SEMAPHORE_SERVICE, proxy_name, object_name
            )

    def _get_group_id(self, proxy_name):
        codec = cp_group_create_cp_group_codec
        request = codec.encode_request(proxy_name)
        invocation = Invocation(request, response_handler=codec.decode_response)
        invocation_service = self._context.invocation_service
        invocation_service.invoke(invocation)
        return invocation.future.result()


class _SessionState(object):
    __slots__ = ("id", "group_id", "ttl", "creation_time", "acquire_count")

    def __init__(self, state_id, group_id, ttl):
        self.id = state_id
        self.ttl = ttl
        self.group_id = group_id
        self.creation_time = time.time()
        self.acquire_count = AtomicInteger()

    def acquire(self, count):
        self.acquire_count.add(count)
        return self.id

    def release(self, count):
        self.acquire_count.add(-count)

    def is_valid(self):
        return self.is_in_use() or not self._is_expired(time.time())

    def is_in_use(self):
        return self.acquire_count.get() > 0

    def _is_expired(self, timestamp):
        expiration_time = self.creation_time + self.ttl
        if expiration_time < 0:
            expiration_time = six.MAXSIZE
        return timestamp > expiration_time

    def __eq__(self, other):
        return isinstance(other, _SessionState) and self.id == other.id

    def __ne__(self, other):
        return not self.__eq__(other)


_NO_SESSION_ID = -1


class ProxySessionManager(object):
    def __init__(self, context):
        self._context = context
        self._mutexes = dict()  # RaftGroupId to RLock
        self._sessions = dict()  # RaftGroupId to SessionState
        self._thread_ids = dict()  # (RaftGroupId, thread_id) to global thread id
        self._heartbeat_timer = None
        self._shutdown = False
        self._lock = RLock()

    def get_session_id(self, group_id):
        session = self._sessions.get(group_id, None)
        if session:
            return session.id
        return _NO_SESSION_ID

    def acquire_session(self, group_id, count):
        return self._get_or_create_session(group_id).continue_with(
            lambda state: state.result().acquire(count)
        )

    def release_session(self, group_id, session_id, count):
        session = self._sessions.get(group_id, None)
        if session and session.id == session_id:
            session.release(count)

    def invalidate_session(self, group_id, session_id):
        # called from the reactor thread only
        session = self._sessions.get(group_id, None)
        if session and session.id == session_id:
            self._sessions.pop(group_id, None)

    def get_or_create_unique_thread_id(self, group_id):
        with self._lock:
            if self._shutdown:
                error = HazelcastClientNotActiveError("Session manager is already shut down!")
                return ImmediateExceptionFuture(error)

            key = (group_id, thread_id())
            global_thread_id = self._thread_ids.get(key)
            if global_thread_id:
                return ImmediateFuture(global_thread_id)

            return self._request_generate_thread_id(group_id).continue_with(
                lambda t_id: self._thread_ids.setdefault(key, t_id.result())
            )

    def shutdown(self):
        with self._lock:
            if self._shutdown:
                return ImmediateFuture(None)

            self._shutdown = True
            if self._heartbeat_timer:
                self._heartbeat_timer.cancel()

            futures = []
            for session in list(self._sessions.values()):
                future = self._request_close_session(session.group_id, session.id)
                futures.append(future)

            def clear(_):
                self._sessions.clear()
                self._mutexes.clear()
                self._thread_ids.clear()

            return combine_futures(futures).continue_with(clear)

    def _request_generate_thread_id(self, group_id):
        codec = cp_session_generate_thread_id_codec
        request = codec.encode_request(group_id)
        invocation = Invocation(request, response_handler=codec.decode_response)
        self._context.invocation_service.invoke(invocation)
        return invocation.future

    def _request_close_session(self, group_id, session_id):
        codec = cp_session_close_session_codec
        request = codec.encode_request(group_id, session_id)
        invocation = Invocation(request, response_handler=codec.decode_response)
        self._context.invocation_service.invoke(invocation)
        return invocation.future

    def _get_or_create_session(self, group_id):
        with self._lock:
            if self._shutdown:
                error = HazelcastClientNotActiveError("Session manager is already shut down!")
                return ImmediateExceptionFuture(error)

            session = self._sessions.get(group_id, None)
            if session is None or not session.is_valid():
                with self._mutex(group_id):
                    session = self._sessions.get(group_id)
                    if session is None or not session.is_valid():
                        return self._create_new_session(group_id)
            return ImmediateFuture(session)

    def _create_new_session(self, group_id):
        f = self._request_new_session(group_id)
        return f.continue_with(self._do_create_new_session, group_id)

    def _do_create_new_session(self, response, group_id):
        # called from the reactor thread only
        response = response.result()
        session = _SessionState(response["session_id"], group_id, response["ttl_millis"] / 1000.0)
        self._sessions[group_id] = session
        self._start_heartbeat_timer(response["heartbeat_millis"] / 1000.0)
        return session

    def _request_new_session(self, group_id):
        codec = cp_session_create_session_codec
        request = codec.encode_request(group_id, self._context.name)
        invocation = Invocation(request, response_handler=codec.decode_response)
        self._context.invocation_service.invoke(invocation)
        return invocation.future

    def _mutex(self, group_id):
        mutex = self._mutexes.get(group_id, None)
        if mutex:
            return mutex

        mutex = RLock()
        current = self._mutexes.setdefault(group_id, mutex)
        return current

    def _start_heartbeat_timer(self, period):
        if self._heartbeat_timer is not None:
            return

        def heartbeat():
            if self._shutdown:
                return

            for session in list(self._sessions.values()):
                if session.is_in_use():

                    def cb(heartbeat_future, session=session):
                        if heartbeat_future.is_success():
                            return

                        error = heartbeat_future.exception()
                        if isinstance(error, (SessionExpiredError, CPGroupDestroyedError)):
                            self.invalidate_session(session.group_id, session.id)

                    f = self._request_heartbeat(session.group_id, session.id)
                    f.add_done_callback(cb)

            r = self._context.reactor
            self._heartbeat_timer = r.add_timer(period, heartbeat)

        reactor = self._context.reactor
        self._heartbeat_timer = reactor.add_timer(period, heartbeat)

    def _request_heartbeat(self, group_id, session_id):
        codec = cp_session_heartbeat_session_codec
        request = codec.encode_request(group_id, session_id)
        invocation = Invocation(request)
        self._context.invocation_service.invoke(invocation)
        return invocation.future
