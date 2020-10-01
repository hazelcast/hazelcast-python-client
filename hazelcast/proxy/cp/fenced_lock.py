import time
import uuid

from hazelcast.errors import LockOwnershipLostError, LockAcquireLimitReachedError, SessionExpiredError, \
    WaitKeyCancelledError, IllegalMonitorStateError
from hazelcast.future import ImmediateExceptionFuture
from hazelcast.protocol.codec import fenced_lock_lock_codec, fenced_lock_try_lock_codec, fenced_lock_unlock_codec, \
    fenced_lock_get_lock_ownership_codec
from hazelcast.proxy.cp import SessionAwareCPProxy
from hazelcast.util import thread_id, to_millis

_NO_SESSION_ID = -1


class FencedLock(SessionAwareCPProxy):
    """A linearizable, distributed lock.

    FencedLock is CP with respect to the CAP principle. It works on top
    of the Raft consensus algorithm. It offers linearizability during crash-stop
    failures and network partitions. If a network partition occurs, it remains
    available on at most one side of the partition.

    FencedLock works on top of CP sessions. Please refer to CP Session
    IMDG documentation section for more information.

    By default, {@link FencedLock} is reentrant. Once a caller acquires
    the lock, it can acquire the lock reentrantly as many times as it wants
    in a linearizable manner. You can configure the reentrancy behaviour
    on the member side. For instance, reentrancy can be disabled and
    FencedLock can work as a non-reentrant mutex. One can also set
    a custom reentrancy limit. When the reentrancy limit is reached,
    FencedLock does not block a lock call. Instead, it fails with
    ``LockAcquireLimitReachedException`` or a specified return value.
    Please check the locking methods to see details about the behaviour.
    """

    INVALID_FENCE = 0

    def __init__(self, context, group_id, service_name, proxy_name, object_name):
        super(FencedLock, self).__init__(context, group_id, service_name, proxy_name, object_name)
        self._lock_session_ids = dict()  # thread-id to session id that has acquired the lock

    def lock(self):
        """Acquires the lock.

        When the caller already holds the lock and the current ``lock()`` call is
        reentrant, the call can fail with ``LockAcquireLimitReachedException``
        if the lock acquire limit is already reached.

        If the lock is not available then the current thread becomes disabled
        for thread scheduling purposes and lies dormant until the lock has been
        acquired.

        Returns:
            hazelcast.future.Future[None]:

        Raises:
            LockOwnershipLostError: If the underlying CP session was
                closed before the client releases the lock
            LockAcquireLimitReachedException: If the lock call is reentrant
                and the configured lock acquire limit is already reached.
        """
        def handler(f):
            f.result()
            return None

        return self.lock_and_get_fence().continue_with(handler)

    def lock_and_get_fence(self):
        """Acquires the lock and returns the fencing token assigned to the current
        thread for this lock acquire.

        If the lock is acquired reentrantly, the same fencing token is returned,
        or the ``lock()`` call can fail with ``LockAcquireLimitReachedException``
        if the lock acquire limit is already reached.

        If the lock is not available then the current thread becomes disabled
        for thread scheduling purposes and lies dormant until the lock has been
        acquired.

        This is a convenience method for the following pattern ::

            lock = client.cp_subsystem.get_lock("lock").blocking()
            lock.lock()
            fence = lock.get_fence()

        Fencing tokens are monotonic numbers that are incremented each time
        the lock switches from the free state to the acquired state. They are
        simply used for ordering lock holders. A lock holder can pass
        its fencing to the shared resource to fence off previous lock holders.
        When this resource receives an operation, it can validate the fencing
        token in the operation.

        Consider the following scenario where the lock is free initially ::

            lock = client.cp_subsystem.get_lock("lock").blocking()
            fence1 = lock.lock_and_get_fence()  # (1)
            fence2 = lock.lock_and_get_fence()  # (2)
            assert fence1 == fence2
            lock.unlock()
            lock.unlock()
            fence3 = lock.lock_and_get_fence()  # (3)
            assert fence3 > fence1

        In this scenario, the lock is acquired by a thread in the cluster. Then,
        the same thread reentrantly acquires the lock again. The fencing token
        returned from the second acquire is equal to the one returned from the
        first acquire, because of reentrancy. After the second acquire, the lock
        is released 2 times, hence becomes free. There is a third lock acquire
        here, which returns a new fencing token. Because this last lock acquire
        is not reentrant, its fencing token is guaranteed to be larger than the
        previous tokens, independent of the thread that has acquired the lock.

        Returns:
            hazelcast.future.Future[int]: The fencing token.

        Raises:
            LockOwnershipLostError: If the underlying CP session was
                closed before the client releases the lock
            LockAcquireLimitReachedException: If the lock call is reentrant
                and the configured lock acquire limit is already reached.
        """
        current_thread_id = thread_id()
        invocation_uuid = uuid.uuid4()
        return self._do_lock(current_thread_id, invocation_uuid)

    def try_lock(self, timeout=0):
        """Acquires the lock if it is free within the given waiting time,
        or already held by the current thread.

        If the lock is available, this method returns immediately with the value
        ``True``. When the call is reentrant, it immediately returns
        ``True`` if the lock acquire limit is not exceeded. Otherwise,
        it returns ``False`` on the reentrant lock attempt if the acquire
        limit is exceeded.

        If the lock is not available then the current thread becomes disabled
        for thread scheduling purposes and lies dormant until the lock is
        acquired by the current thread or the specified waiting time elapses.

        If the lock is acquired, then the value ``True`` is returned.

        If the specified waiting time elapses, then the value ``False``
        is returned. If the time is less than or equal to zero, the method does
        not wait at all. By default, timeout is set to zero.

        A typical usage idiom for this method would be ::

            lock = client.cp_subsystem.get_lock("lock").blocking()
            if lock.try_lock():
                try:
                    # manipulate the protected state
                finally:
                    lock.unlock()
            else:
                # perform another action

        This usage ensures that the lock is unlocked if it was acquired,
        and doesn't try to unlock if the lock was not acquired.

        Args:
            timeout (int): The maximum time to wait for the lock in seconds.

        Returns:
            hazelcast.future.Future[bool]: ``True`` if the lock was acquired and ``False``
                if the waiting time elapsed before the lock was acquired.

        Raises:
            LockOwnershipLostError: If the underlying CP session was
                closed before the client releases the lock
        """
        return self.try_lock_and_get_fence(timeout).continue_with(lambda f: f.result() != self.INVALID_FENCE)

    def try_lock_and_get_fence(self, timeout=0):
        """Acquires the lock if it is free within the given waiting time,
        or already held by the current thread at the time of invocation and,
        the acquire limit is not exceeded, and returns the fencing token
        assigned to the current thread for this lock acquire.

        If the lock is acquired reentrantly, the same fencing token is returned.
        If the lock acquire limit is exceeded, then this method immediately returns
        :const:`INVALID_FENCE` that represents a failed lock attempt.

        If the lock is not available then the current thread becomes disabled
        for thread scheduling purposes and lies dormant until the lock is
        acquired by the current thread or the specified waiting time elapses.

        If the specified waiting time elapses, then :const:`INVALID_FENCE`
        is returned. If the time is less than or equal to zero, the method does
        not wait at all. By default, timeout is set to zero.

        This is a convenience method for the following pattern ::

            lock = client.cp_subsystem.get_lock("lock").blocking()
            if lock.try_lock():
                fence = lock.get_fence()
            else:
                fence = lock.INVALID_FENCE

        See Also:
            :func:`lock_and_get_fence` function for more information about fences.

        Args:
            timeout (int): The maximum time to wait for the lock in seconds.

        Returns:
            hazelcast.future.Future[int]: The fencing token if the lock was acquired and
                :const:`INVALID_FENCE` otherwise.

        Raises:
            LockOwnershipLostError: If the underlying CP session was
                closed before the client releases the lock
        """
        current_thread_id = thread_id()
        invocation_uuid = uuid.uuid4()
        timeout = max(0, timeout)
        return self._do_try_lock(current_thread_id, invocation_uuid, timeout)

    def unlock(self):
        """Releases the lock if the lock is currently held by the current thread.

        Returns:
            hazelcast.future.Future[None]:

        Raises:
            LockOwnershipLostError: If the underlying CP session was
                closed before the client releases the lock
            IllegalMonitorStateError: If the lock is not held by
                the current thread
        """
        current_thread_id = thread_id()
        session_id = self._get_session_id()

        # the order of the following checks is important
        try:
            self._verify_locked_session_id_if_present(current_thread_id, session_id, False)
        except LockOwnershipLostError as e:
            return ImmediateExceptionFuture(e)

        if session_id == _NO_SESSION_ID:
            self._lock_session_ids.pop(current_thread_id, None)
            return ImmediateExceptionFuture(self._new_illegal_monitor_state_error())

        def check_response(f):
            try:
                still_locked_by_the_current_thread = f.result()
                if still_locked_by_the_current_thread:
                    self._lock_session_ids[current_thread_id] = session_id
                else:
                    self._lock_session_ids.pop(current_thread_id, None)

                self._release_session(session_id)
            except SessionExpiredError:
                self._invalidate_session(session_id)
                self._lock_session_ids.pop(current_thread_id, None)
                raise self._new_lock_ownership_lost_error(session_id)
            except IllegalMonitorStateError as e:
                self._lock_session_ids.pop(current_thread_id, None)
                raise e

        return self._request_unlock(session_id, current_thread_id, uuid.uuid4()).continue_with(check_response)

    def get_fence(self):
        """Returns the fencing token if the lock is held by the current thread.

        Fencing tokens are monotonic numbers that are incremented each time
        the lock switches from the free state to the acquired state. They are
        simply used for ordering lock holders. A lock holder can pass
        its fencing to the shared resource to fence off previous lock holders.
        When this resource receives an operation, it can validate the fencing
        token in the operation.

        Returns:
            hazelcast.future.Future[int]: The fencing token if the lock is held
                by the current thread

        Raises:
            LockOwnershipLostError: If the underlying CP session was
                closed before the client releases the lock
            IllegalMonitorStateError: If the lock is not held by
                the current thread
        """
        current_thread_id = thread_id()
        session_id = self._get_session_id()

        # the order of the following checks is important
        try:
            self._verify_locked_session_id_if_present(current_thread_id, session_id, False)
        except LockOwnershipLostError as e:
            return ImmediateExceptionFuture(e)

        if session_id == _NO_SESSION_ID:
            self._lock_session_ids.pop(current_thread_id, None)
            return ImmediateExceptionFuture(self._new_illegal_monitor_state_error())

        def check_response(f):
            state = _LockOwnershipState(f.result())
            if state.is_locked_by(session_id, current_thread_id):
                self._lock_session_ids[current_thread_id] = session_id
                return state.fence

            self._verify_no_locked_session_id_present(current_thread_id)
            raise self._new_illegal_monitor_state_error()

        return self._request_get_lock_ownership_state().continue_with(check_response)

    def is_locked(self):
        """Returns whether this lock is locked or not.

        Returns:
            hazelcast.future.Future[bool]: ``True`` if this lock is locked by any thread
                in the cluster, ``False`` otherwise.

        Raises:
            LockOwnershipLostError: If the underlying CP session was
                closed before the client releases the lock
        """
        current_thread_id = thread_id()
        session_id = self._get_session_id()

        try:
            self._verify_locked_session_id_if_present(current_thread_id, session_id, False)
        except LockOwnershipLostError as e:
            return ImmediateExceptionFuture(e)

        def check_response(f):
            state = _LockOwnershipState(f.result())
            if state.is_locked_by(session_id, current_thread_id):
                self._lock_session_ids[current_thread_id] = session_id
                return True

            self._verify_no_locked_session_id_present(current_thread_id)
            return state.is_locked()

        return self._request_get_lock_ownership_state().continue_with(check_response)

    def is_locked_by_current_thread(self):
        """Returns whether the lock is held by the current thread or not.

        Returns:
            hazelcast.future.Future[bool]: ``True`` if the lock is held by the current thread,
                ``False`` otherwise.

        Raises:
            LockOwnershipLostError: If the underlying CP session was
                closed before the client releases the lock
        """
        current_thread_id = thread_id()
        session_id = self._get_session_id()

        try:
            self._verify_locked_session_id_if_present(current_thread_id, session_id, False)
        except LockOwnershipLostError as e:
            return ImmediateExceptionFuture(e)

        def check_response(f):
            state = _LockOwnershipState(f.result())
            locked_by_the_current_thread = state.is_locked_by(session_id, current_thread_id)
            if locked_by_the_current_thread:
                self._lock_session_ids[current_thread_id] = session_id
            else:
                self._verify_no_locked_session_id_present(current_thread_id)

            return locked_by_the_current_thread

        return self._request_get_lock_ownership_state().continue_with(check_response)

    def get_lock_count(self):
        """Returns the reentrant lock count if the lock is held by any thread
        in the cluster.

        Returns:
            hazelcast.future.Future[int]: The reentrant lock count if the lock is held
            by any thread in the cluster

        Raises:
            LockOwnershipLostError: If the underlying CP session was
                closed before the client releases the lock
        """
        current_thread_id = thread_id()
        session_id = self._get_session_id()

        try:
            self._verify_locked_session_id_if_present(current_thread_id, session_id, False)
        except LockOwnershipLostError as e:
            return ImmediateExceptionFuture(e)

        def check_response(f):
            state = _LockOwnershipState(f.result())
            if state.is_locked_by(session_id, current_thread_id):
                self._lock_session_ids[current_thread_id] = session_id
            else:
                self._verify_no_locked_session_id_present(current_thread_id)

            return state.lock_count

        return self._request_get_lock_ownership_state().continue_with(check_response)

    def destroy(self):
        self._lock_session_ids.clear()
        return super(FencedLock, self).destroy()

    def _do_lock(self, current_thread_id, invocation_uuid):
        def do_lock_once(session_id):
            session_id = session_id.result()
            self._verify_locked_session_id_if_present(current_thread_id, session_id, True)

            def check_fence(fence):
                try:
                    fence = fence.result()
                except SessionExpiredError:
                    self._invalidate_session(session_id)
                    self._verify_no_locked_session_id_present(current_thread_id)
                    return self._do_lock(current_thread_id, invocation_uuid)
                except WaitKeyCancelledError:
                    self._release_session(session_id)
                    error = IllegalMonitorStateError("Lock(%s) not acquired because the lock call on the CP group "
                                                     "is cancelled, possibly because of another indeterminate call "
                                                     "from the same thread." % self._object_name)
                    raise error
                except Exception as e:
                    self._release_session(session_id)
                    raise e

                if fence != self.INVALID_FENCE:
                    self._lock_session_ids[current_thread_id] = session_id
                    return fence

                self._release_session(session_id)
                error = LockAcquireLimitReachedError("Lock(%s) reentrant lock limit is already reached!"
                                                     % self._object_name)
                raise error

            return self._request_lock(session_id, current_thread_id, invocation_uuid).continue_with(check_fence)

        return self._acquire_session().continue_with(do_lock_once)

    def _do_try_lock(self, current_thread_id, invocation_uuid, timeout):
        start = time.time()

        def do_try_lock_once(session_id):
            session_id = session_id.result()
            self._verify_locked_session_id_if_present(current_thread_id, session_id, True)

            def check_fence(fence):
                try:
                    fence = fence.result()
                except SessionExpiredError:
                    self._invalidate_session(session_id)
                    self._verify_no_locked_session_id_present(current_thread_id)

                    remaining_timeout = timeout - (time.time() - start)
                    if remaining_timeout <= 0:
                        return self.INVALID_FENCE
                    return self._do_try_lock(current_thread_id, invocation_uuid, remaining_timeout)
                except WaitKeyCancelledError:
                    self._release_session(session_id)
                    return self.INVALID_FENCE
                except Exception as e:
                    self._release_session(session_id)
                    raise e

                if fence != self.INVALID_FENCE:
                    self._lock_session_ids[current_thread_id] = session_id
                else:
                    self._release_session(session_id)

                return fence

            return self._request_try_lock(session_id, current_thread_id, invocation_uuid, timeout).continue_with(
                check_fence)

        return self._acquire_session().continue_with(do_try_lock_once)

    def _verify_locked_session_id_if_present(self, current_thread_id, session_id, release_session):
        lock_session_id = self._lock_session_ids.get(current_thread_id, None)
        if lock_session_id and lock_session_id != session_id:
            self._lock_session_ids.pop(current_thread_id, None)
            if release_session:
                self._release_session(session_id)

            raise self._new_lock_ownership_lost_error(lock_session_id)

    def _verify_no_locked_session_id_present(self, current_thread_id):
        lock_session_id = self._lock_session_ids.pop(current_thread_id, None)
        if lock_session_id:
            raise self._new_lock_ownership_lost_error(lock_session_id)

    def _new_lock_ownership_lost_error(self, lock_session_id):
        error = LockOwnershipLostError("Current thread is not the owner of the Lock(%s) because its "
                                       "Session(%s) is closed by the server." % (self._proxy_name, lock_session_id))
        return error

    def _new_illegal_monitor_state_error(self):
        error = IllegalMonitorStateError("Current thread is not the owner of the Lock(%s)" % self._proxy_name)
        return error

    def _request_lock(self, session_id, current_thread_id, invocation_uuid):
        codec = fenced_lock_lock_codec
        request = codec.encode_request(self._group_id, self._object_name, session_id, current_thread_id,
                                       invocation_uuid)
        return self._invoke(request, codec.decode_response)

    def _request_try_lock(self, session_id, current_thread_id, invocation_uuid, timeout):
        codec = fenced_lock_try_lock_codec
        request = codec.encode_request(self._group_id, self._object_name, session_id, current_thread_id,
                                       invocation_uuid, to_millis(timeout))
        return self._invoke(request, codec.decode_response)

    def _request_unlock(self, session_id, current_thread_id, invocation_uuid):
        codec = fenced_lock_unlock_codec
        request = codec.encode_request(self._group_id, self._object_name, session_id, current_thread_id,
                                       invocation_uuid)
        return self._invoke(request, codec.decode_response)

    def _request_get_lock_ownership_state(self):
        codec = fenced_lock_get_lock_ownership_codec
        request = codec.encode_request(self._group_id, self._object_name)
        return self._invoke(request, codec.decode_response)


class _LockOwnershipState(object):
    def __init__(self, state):
        self.fence = state["fence"]
        self.lock_count = state["lock_count"]
        self.session_id = state["session_id"]
        self.thread_id = state["thread_id"]

    def is_locked(self):
        return self.fence != FencedLock.INVALID_FENCE

    def is_locked_by(self, session_id, current_thread_id):
        return self.is_locked() and self.session_id == session_id and self.thread_id == current_thread_id
