import time
import uuid

from hazelcast.errors import SessionExpiredError, WaitKeyCancelledError, IllegalStateError
from hazelcast.future import ImmediateFuture, ImmediateExceptionFuture
from hazelcast.protocol.codec import (
    semaphore_init_codec,
    semaphore_acquire_codec,
    semaphore_available_permits_codec,
    semaphore_drain_codec,
    semaphore_change_codec,
    semaphore_release_codec,
)
from hazelcast.proxy.cp import SessionAwareCPProxy, BaseCPProxy
from hazelcast.util import check_not_negative, check_true, thread_id, to_millis

_DRAIN_SESSION_ACQ_COUNT = 1024
"""
Since a proxy does not know how many permits will be drained on
the Raft group, it uses this constant to increment its local session
acquire count. Then, it adjusts the local session acquire count after
the drain response is returned.
"""

_NO_SESSION_ID = -1


class Semaphore(BaseCPProxy):
    """A linearizable, distributed semaphore.

    Semaphores are often used to restrict the number of callers that can access
    some physical or logical resource.

    Semaphore is a cluster-wide counting semaphore. Conceptually, it maintains
    a set of permits. Each ``acquire()`` blocks if necessary until a permit
    is available, and then takes it. Dually, each ``release()`` adds a
    permit, potentially releasing a blocking acquirer. However, no actual permit
    objects are used; the semaphore just keeps a count of the number available
    and acts accordingly.

    Hazelcast's distributed semaphore implementation guarantees that callers
    invoking any of the ``acquire()`` methods are selected to
    obtain permits in the order of their invocations (first-in-first-out; FIFO).
    Note that FIFO ordering implies the order which the primary replica of an
    Semaphore receives these acquire requests. Therefore, it is
    possible for one member to invoke ``acquire()`` before another member,
    but its request hits the primary replica after the other member.

    This class also provides convenient ways to work with multiple permits at once.
    Beware of the increased risk of indefinite postponement when using the
    multiple-permit acquire. If permits are released one by one, a caller waiting
    for one permit will acquire it before a caller waiting for multiple permits
    regardless of the call order.

    Correct usage of a semaphore is established by programming convention
    in the application.

    It works on top of the Raft consensus algorithm. It offers linearizability during crash
    failures and network partitions. It is CP with respect to the CAP principle.
    If a network partition occurs, it remains available on at most one side of
    the partition.

    It has 2 variations:

    - The default implementation accessed via ``cp_subsystem`` is session-aware. In this
      one, when a caller makes its very first ``acquire()`` call, it starts
      a new CP session with the underlying CP group. Then, liveliness of the
      caller is tracked via this CP session. When the caller fails, permits
      acquired by this caller are automatically and safely released. However,
      the session-aware version comes with a limitation, that is, a client cannot
      release permits before acquiring them first. In other words, a client can release
      only the permits it has acquired earlier. It means, you can acquire a permit
      from one thread and release it from another thread using the same Hazelcast client,
      but not different instances of Hazelcast client. You can use the session-aware
      CP Semaphore implementation by disabling JDK compatibility via ``jdk-compatible``
      server-side setting. Although the session-aware implementation has a minor
      difference to the JDK Semaphore, we think it is a better fit for distributed
      environments because of its safe auto-cleanup mechanism for acquired permits.
    - The second implementation offered by ``cp_subsystem`` is sessionless. This
      implementation does not perform auto-cleanup of acquired permits on failures.
      Acquired permits are not bound to threads and permits can be released without
      acquiring first. However, you need to handle failed permit owners on your own.
      If a Hazelcast server or a client fails while holding some permits, they will not be
      automatically released. You can use the sessionless CP Semaphore implementation
      by enabling JDK compatibility via ``jdk-compatible`` server-side setting.

    There is a subtle difference between the lock and semaphore abstractions.
    A lock can be assigned to at most one endpoint at a time, so we have a total
    order among its holders. However, permits of a semaphore can be assigned to
    multiple endpoints at a time, which implies that we may not have a total
    order among permit holders. In fact, permit holders are partially ordered.
    For this reason, the fencing token approach, which is explained in
    :class:`~hazelcast.proxy.cp.fenced_lock.FencedLock`, does not work for the
    semaphore abstraction. Moreover, each permit is an independent entity.
    Multiple permit acquires and reentrant lock acquires of a single endpoint are
    not equivalent. The only case where a semaphore behaves like a lock is the
    binary case, where the semaphore has only 1 permit. In this case, the semaphore
    works like a non-reentrant lock.

    All of the API methods in the new CP Semaphore implementation offer
    the exactly-once execution semantics for the session-aware version.
    For instance, even if a ``release()`` call is internally retried
    because of a crashed Hazelcast member, the permit is released only once.
    However, this guarantee is not given for the sessionless, a.k.a,
    JDK-compatible CP Semaphore.
    """

    def init(self, permits):
        """Tries to initialize this Semaphore instance with the given permit count.

        Args:
            permits (int): The given permit count.

        Returns:
            hazelcast.future.Future[bool]: ``True`` if the initialization succeeds,
            ``False`` if already initialized.

        Raises:
            AssertionError: If the ``permits`` is negative.
        """
        check_not_negative(permits, "Permits must be non-negative")
        codec = semaphore_init_codec
        request = codec.encode_request(self._group_id, self._object_name, permits)
        return self._invoke(request, codec.decode_response)

    def acquire(self, permits=1):
        """Acquires the given number of permits if they are available,
        and returns immediately, reducing the number of available permits
        by the given amount.

        If insufficient permits are available then the result of the returned
        future is not set until one of the following things happens:

        - Some other caller invokes one of the ``release``
          methods for this semaphore, the current caller is next to be assigned
          permits and the number of available permits satisfies this request,
        - This Semaphore instance is destroyed

        Args:
            permits (int): Optional number of permits to acquire; defaults to ``1``
                when not specified

        Returns:
            hazelcast.future.Future[None]:

        Raises:
            AssertionError: If the ``permits`` is not positive.
        """
        raise NotImplementedError("acquire")

    def available_permits(self):
        """Returns the current number of permits currently available in this semaphore.

        This method is typically used for debugging and testing purposes.

        Returns:
            hazelcast.future.Future[int]: The number of permits available in this semaphore.
        """
        codec = semaphore_available_permits_codec
        request = codec.encode_request(self._group_id, self._object_name)
        return self._invoke(request, codec.decode_response)

    def drain_permits(self):
        """Acquires and returns all permits that are available at invocation time.

        Returns:
            hazelcast.future.Future[int]: The number of permits drained.
        """
        raise NotImplementedError("drain_permits")

    def reduce_permits(self, reduction):
        """Reduces the number of available permits by the indicated amount.

        This method differs from ``acquire`` as it does not block until permits
        become available. Similarly, if the caller has acquired some permits,
        they are not released with this call.

        Args:
            reduction (int): The number of permits to reduce.

        Returns:
            hazelcast.future.Future[None]:

        Raises:
             AssertionError: If the ``reduction`` is negative.
        """
        check_not_negative(reduction, "Reduction must be non-negative")
        if reduction == 0:
            return ImmediateFuture(None)

        return self._do_change_permits(-reduction)

    def increase_permits(self, increase):
        """Increases the number of available permits by the indicated amount.

        If there are some callers waiting for permits to become available, they
        will be notified. Moreover, if the caller has acquired some permits,
        they are not released with this call.

        Args:
            increase (int): The number of permits to increase.

        Returns:
            hazelcast.future.Future[None]:

        Raises:
            AssertionError: If ``increase`` is negative.
        """
        check_not_negative(increase, "Increase must be non-negative")
        if increase == 0:
            return ImmediateFuture(None)

        return self._do_change_permits(increase)

    def release(self, permits=1):
        """Releases the given number of permits and increases the number of
        available permits by that amount.

        If some callers in the cluster are blocked for acquiring permits,
        they will be notified.

        If the underlying Semaphore implementation is non-JDK-compatible
        (configured via ``jdk-compatible`` server-side setting), then a
        client can only release a permit which it has acquired before.
        In other words, a client cannot release a permit without acquiring
        it first.

        Otherwise, which means the underlying implementation is JDK compatible
        (configured via ``jdk-compatible`` server-side setting), there is no
        requirement that a client that releases a permit must have acquired
        that permit by calling one of the ``acquire()`` methods. A client can
        freely release a permit without acquiring it first. In this case,
        correct usage of a semaphore is established by programming convention
        in the application.

        Args:
            permits (int): Optional number of permits to release; defaults to ``1``
                when not specified.

        Returns:
            hazelcast.future.Future[None]:

        Raises:
            AssertionError: If the ``permits`` is not positive.
            IllegalStateError: if the Semaphore is non-JDK-compatible and the caller
                does not have a permit
        """
        raise NotImplementedError("release")

    def try_acquire(self, permits=1, timeout=0):
        """Acquires the given number of permits and returns ``True``, if they
        become available during the given waiting time.

        If permits are acquired, the number of available permits in the Semaphore
        instance is also reduced by the given amount.

        If no sufficient permits are available, then the result of the returned
        future is not set until one of the following things happens:

        - Permits are released by other callers, the current caller is next to
          be assigned permits and the number of available permits satisfies this
          request
        - The specified waiting time elapses

        Args:
            permits (int): The number of permits to acquire; defaults to ``1``
                when not specified.
            timeout (int): Optional timeout in seconds to wait for the permits;
                when it's not specified the operation will return
                immediately after the acquire attempt

        Returns:
            hazelcast.future.Future[bool]: ``True`` if all permits were acquired,
            ``False`` if the waiting time elapsed before all permits could be
            acquired

        Raises:
            AssertionError: If the ``permits`` is not positive.
        """
        raise NotImplementedError("try_acquire")

    def _do_change_permits(self, permits):
        raise NotImplementedError("_do_change_permits")


class SessionAwareSemaphore(Semaphore, SessionAwareCPProxy):
    def acquire(self, permits=1):
        check_true(permits > 0, "Permits must be positive")
        current_thread_id = thread_id()
        invocation_uuid = uuid.uuid4()
        return self._do_acquire(current_thread_id, invocation_uuid, permits)

    def drain_permits(self):
        current_thread_id = thread_id()
        invocation_uuid = uuid.uuid4()
        return self._do_drain(current_thread_id, invocation_uuid)

    def release(self, permits=1):
        check_true(permits > 0, "Permits must be positive")
        session_id = self._get_session_id()
        if session_id == _NO_SESSION_ID:
            return ImmediateExceptionFuture(self._new_illegal_state_error())

        current_thread_id = thread_id()
        invocation_uuid = uuid.uuid4()

        def check_response(response):
            try:
                response.result()
            except SessionExpiredError as e:
                self._invalidate_session(session_id)
                raise self._new_illegal_state_error(e)
            finally:
                self._release_session(session_id, permits)

        return self._request_release(
            session_id, current_thread_id, invocation_uuid, permits
        ).continue_with(check_response)

    def try_acquire(self, permits=1, timeout=0):
        check_true(permits > 0, "Permits must be positive")
        timeout = max(0, timeout)
        current_thread_id = thread_id()
        invocation_uuid = uuid.uuid4()
        return self._do_try_acquire(current_thread_id, invocation_uuid, permits, timeout)

    def _do_acquire(self, current_thread_id, invocation_uuid, permits):
        def do_acquire_once(session_id):
            session_id = session_id.result()

            def check_response(response):
                try:
                    response.result()
                except SessionExpiredError:
                    self._invalidate_session(session_id)
                    return self._do_acquire(current_thread_id, invocation_uuid, permits)
                except WaitKeyCancelledError:
                    self._release_session(session_id, permits)
                    error = IllegalStateError(
                        'Semaphore("%s") not acquired because the acquire call on the CP '
                        "group is cancelled, possibly because of another indeterminate call "
                        "from the same thread." % self._object_name
                    )
                    raise error
                except Exception as e:
                    self._release_session(session_id, permits)
                    raise e

            return self._request_acquire(
                session_id, current_thread_id, invocation_uuid, permits, -1
            ).continue_with(check_response)

        return self._acquire_session(permits).continue_with(do_acquire_once)

    def _do_drain(self, current_thread_id, invocation_uuid):
        def do_drain_once(session_id):
            session_id = session_id.result()

            def check_count(count):
                try:
                    count = count.result()
                    self._release_session(session_id, _DRAIN_SESSION_ACQ_COUNT - count)
                    return count
                except SessionExpiredError:
                    self._invalidate_session(session_id)
                    return self._do_drain(current_thread_id, invocation_uuid)
                except Exception as e:
                    self._release_session(session_id, _DRAIN_SESSION_ACQ_COUNT)
                    raise e

            return self._request_drain(
                session_id, current_thread_id, invocation_uuid
            ).continue_with(check_count)

        return self._acquire_session(_DRAIN_SESSION_ACQ_COUNT).continue_with(do_drain_once)

    def _do_change_permits(self, delta):
        current_thread_id = thread_id()
        invocation_uuid = uuid.uuid4()

        def do_change_permits_once(session_id):
            session_id = session_id.result()

            def check_response(response):
                try:
                    response.result()
                except SessionExpiredError as e:
                    self._invalidate_session(session_id)
                    raise self._new_illegal_state_error(e)
                finally:
                    self._release_session(session_id)

            return self._request_change(
                session_id, current_thread_id, invocation_uuid, delta
            ).continue_with(check_response)

        return self._acquire_session().continue_with(do_change_permits_once)

    def _do_try_acquire(self, current_thread_id, invocation_uuid, permits, timeout):
        start = time.time()

        def do_try_acquire_once(session_id):
            session_id = session_id.result()

            def check_response(response):
                try:
                    acquired = response.result()
                    if not acquired:
                        self._release_session(session_id, permits)
                    return acquired
                except SessionExpiredError:
                    self._invalidate_session(session_id)
                    remaining_timeout = timeout - (time.time() - start)
                    if remaining_timeout <= 0:
                        return False
                    return self._do_try_acquire(
                        current_thread_id, invocation_uuid, permits, remaining_timeout
                    )
                except WaitKeyCancelledError:
                    self._release_session(session_id, permits)
                    return False
                except Exception as e:
                    self._release_session(session_id, permits)
                    raise e

            return self._request_acquire(
                session_id, current_thread_id, invocation_uuid, permits, timeout
            ).continue_with(check_response)

        return self._acquire_session(permits).continue_with(do_try_acquire_once)

    def _new_illegal_state_error(self, cause=None):
        error = IllegalStateError(
            'Semaphore["%s"] has no valid session!' % self._object_name, cause
        )
        return error

    def _request_acquire(self, session_id, current_thread_id, invocation_uuid, permits, timeout):
        codec = semaphore_acquire_codec
        if timeout > 0:
            timeout = to_millis(timeout)

        request = codec.encode_request(
            self._group_id,
            self._object_name,
            session_id,
            current_thread_id,
            invocation_uuid,
            permits,
            timeout,
        )
        return self._invoke(request, codec.decode_response)

    def _request_drain(self, session_id, current_thread_id, invocation_uuid):
        codec = semaphore_drain_codec
        request = codec.encode_request(
            self._group_id, self._object_name, session_id, current_thread_id, invocation_uuid
        )
        return self._invoke(request, codec.decode_response)

    def _request_change(self, session_id, current_thread_id, invocation_uuid, delta):
        codec = semaphore_change_codec
        request = codec.encode_request(
            self._group_id, self._object_name, session_id, current_thread_id, invocation_uuid, delta
        )
        return self._invoke(request)

    def _request_release(self, session_id, current_thread_id, invocation_uuid, permits):
        codec = semaphore_release_codec
        request = codec.encode_request(
            self._group_id,
            self._object_name,
            session_id,
            current_thread_id,
            invocation_uuid,
            permits,
        )
        return self._invoke(request)


class SessionlessSemaphore(Semaphore):
    def __init__(self, context, group_id, service_name, proxy_name, object_name):
        super(SessionlessSemaphore, self).__init__(
            context, group_id, service_name, proxy_name, object_name
        )
        self._session_manager = context.proxy_session_manager

    def acquire(self, permits=1):
        check_true(permits > 0, "Permits must be positive")

        def handler(f):
            f.result()
            return None

        return (
            self._get_thread_id()
            .continue_with(self._do_try_acquire, permits, -1)
            .continue_with(handler)
        )

    def drain_permits(self):
        return self._get_thread_id().continue_with(self._do_drain_permits)

    def release(self, permits=1):
        check_true(permits > 0, "Permits must be positive")
        invocation_uuid = uuid.uuid4()
        return self._get_thread_id().continue_with(self._request_release, invocation_uuid, permits)

    def try_acquire(self, permits=1, timeout=0):
        check_true(permits > 0, "Permits must be positive")
        timeout = max(0, timeout)
        return self._get_thread_id().continue_with(self._do_try_acquire, permits, timeout)

    def _do_try_acquire(self, global_thread_id, permits, timeout):
        global_thread_id = global_thread_id.result()
        invocation_uuid = uuid.uuid4()
        return self._request_acquire(
            global_thread_id, invocation_uuid, permits, timeout
        ).continue_with(self._check_acquire_response)

    def _do_drain_permits(self, global_thread_id):
        global_thread_id = global_thread_id.result()
        invocation_uuid = uuid.uuid4()
        codec = semaphore_drain_codec
        request = codec.encode_request(
            self._group_id, self._object_name, _NO_SESSION_ID, global_thread_id, invocation_uuid
        )
        return self._invoke(request, codec.decode_response)

    def _do_change_permits(self, permits):
        invocation_uuid = uuid.uuid4()
        return self._get_thread_id().continue_with(self._request_change, invocation_uuid, permits)

    def _request_acquire(self, global_thread_id, invocation_uuid, permits, timeout):
        codec = semaphore_acquire_codec
        if timeout > 0:
            timeout = to_millis(timeout)

        request = codec.encode_request(
            self._group_id,
            self._object_name,
            _NO_SESSION_ID,
            global_thread_id,
            invocation_uuid,
            permits,
            timeout,
        )
        return self._invoke(request, codec.decode_response)

    def _request_change(self, global_thread_id, invocation_uuid, permits):
        global_thread_id = global_thread_id.result()
        codec = semaphore_change_codec
        request = codec.encode_request(
            self._group_id,
            self._object_name,
            _NO_SESSION_ID,
            global_thread_id,
            invocation_uuid,
            permits,
        )
        return self._invoke(request)

    def _request_release(self, global_thread_id, invocation_uuid, permits):
        global_thread_id = global_thread_id.result()
        codec = semaphore_release_codec
        request = codec.encode_request(
            self._group_id,
            self._object_name,
            _NO_SESSION_ID,
            global_thread_id,
            invocation_uuid,
            permits,
        )
        return self._invoke(request)

    def _check_acquire_response(self, response):
        try:
            return response.result()
        except WaitKeyCancelledError:
            error = IllegalStateError(
                'Semaphore("%s") not acquired because the acquire call on the '
                "CP group is cancelled, possibly because of another indeterminate "
                "call from the same thread." % self._object_name
            )
            raise error

    def _get_thread_id(self):
        return self._session_manager.get_or_create_unique_thread_id(self._group_id)
