import time
import uuid

from hazelcast.errors import (
    LockOwnershipLostError,
    IllegalMonitorStateError,
    SessionExpiredError,
    WaitKeyCancelledError,
    LockAcquireLimitReachedError,
)
from hazelcast.internal.asyncio_proxy.base import task_id
from hazelcast.internal.asyncio_proxy.cp import SessionAwareCPProxy
from hazelcast.protocol.codec import (
    fenced_lock_unlock_codec,
    fenced_lock_get_lock_ownership_codec,
    fenced_lock_try_lock_codec,
    fenced_lock_lock_codec,
)
from hazelcast.proxy.cp.fenced_lock import _LockOwnershipState
from hazelcast.util import to_millis

_NO_SESSION_ID = -1


class FencedLock(SessionAwareCPProxy):
    """A linearizable, distributed lock.

    FencedLock is CP with respect to the CAP principle. It works on top
    of the Raft consensus algorithm. It offers linearizability during crash-stop
    failures and network partitions. If a network partition occurs, it remains
    available on at most one side of the partition.

    FencedLock works on top of CP sessions. Please refer to CP Session
    documentation section for more information.

    By default, FencedLock is reentrant. Once a caller acquires
    the lock, it can acquire the lock reentrantly as many times as it wants
    in a linearizable manner. You can configure the reentrancy behaviour
    on the member side. For instance, reentrancy can be disabled and
    FencedLock can work as a non-reentrant mutex. One can also set
    a custom reentrancy limit. When the reentrancy limit is reached,
    FencedLock does not block a lock call. Instead, it fails with
    ``LockAcquireLimitReachedError`` or a specified return value.
    Please check the locking methods to see details about the behaviour.
    """

    INVALID_FENCE = 0

    def __init__(self, context, group_id, service_name, proxy_name, object_name):
        super(FencedLock, self).__init__(context, group_id, service_name, proxy_name, object_name)
        self._lock_session_ids = dict()  # thread-id to session id that has acquired the lock

    async def lock(self) -> int:
        """Acquires the lock and returns the fencing token assigned to the
        current task.

        If the lock is acquired reentrantly, the same fencing token is returned,
        or the ``lock()`` call can fail with ``LockAcquireLimitReachedError``
        if the lock acquire limit is already reached.

        If the lock is not available then the current task becomes disabled
        for thread scheduling purposes and lies dormant until the lock has been
        acquired.

        Fencing tokens are monotonic numbers that are incremented each time
        the lock switches from the free state to the acquired state. They are
        simply used for ordering lock holders. A lock holder can pass
        its fencing to the shared resource to fence off previous lock holders.
        When this resource receives an operation, it can validate the fencing
        token in the operation.

        Consider the following scenario where the lock is free initially ::

            lock = await client.cp_subsystem.get_lock("lock")
            fence1 = await lock.lock()  # (1)
            fence2 = await lock.lock()  # (2)
            assert fence1 == fence2
            await lock.unlock()
            await lock.unlock()
            fence3 = await lock.lock()  # (3)
            assert fence3 > fence1

        In this scenario, the lock is acquired by a task in the cluster. Then,
        the same task reentrantly acquires the lock again. The fencing token
        returned from the second acquire is equal to the one returned from the
        first acquire, because of reentrancy. After the second acquire, the lock
        is released 2 times, hence becomes free. There is a third lock acquire
        here, which returns a new fencing token. Because this last lock acquire
        is not reentrant, its fencing token is guaranteed to be larger than the
        previous tokens, independent of the task that has acquired the lock.

        Returns:
            The fencing token.

        Raises:
            LockOwnershipLostError: If the underlying CP session was
                closed before the client releases the lock
            LockAcquireLimitReachedError: If the lock call is reentrant
                and the configured lock acquire limit is already reached.
        """
        invocation_uuid = uuid.uuid4()
        return await self._do_lock(task_id(), invocation_uuid)

    async def try_lock(self, timeout: float = 0) -> int:
        """Acquires the lock if it is free within the given waiting time,
        or already held by the current task at the time of invocation and,
        the acquire limit is not exceeded, and returns the fencing token
        assigned to the current task.

        If the lock is acquired reentrantly, the same fencing token is returned.
        If the lock acquire limit is exceeded, then this method immediately
        returns :const:`INVALID_FENCE` that represents a failed lock attempt.

        If the lock is not available then the current task becomes disabled
        for scheduling purposes and lies dormant until the lock is
        acquired by the current task or the specified waiting time elapses.

        If the specified waiting time elapses, then :const:`INVALID_FENCE`
        is returned. If the time is less than or equal to zero, the method does
        not wait at all. By default, timeout is set to zero.

        A typical usage idiom for this method would be ::

            lock = await client.cp_subsystem.get_lock("lock")
            fence = await lock.try_lock()
            if fence != lock.INVALID_FENCE:
                try:
                    # manipulate the protected state
                finally:
                    await lock.unlock()
            else:
                # perform another action

        This usage ensures that the lock is unlocked if it was acquired,
        and doesn't try to unlock if the lock was not acquired.

        See Also:
            :func:`lock` function for more information about fences.

        Args:
            timeout: The maximum time to wait for the lock in seconds.

        Returns:
            The fencing token if the lock was acquired and
            :const:`INVALID_FENCE` otherwise.

        Raises:
            LockOwnershipLostError: If the underlying CP session was
                closed before the client releases the lock
        """
        invocation_uuid = uuid.uuid4()
        timeout = max(0.0, timeout)
        return await self._do_try_lock(task_id(), invocation_uuid, timeout)

    async def unlock(self) -> None:
        """Releases the lock if the lock is currently held by the current
        task.

        Raises:
            LockOwnershipLostError: If the underlying CP session was
                closed before the client releases the lock
            IllegalMonitorStateError: If the lock is not held by
                the current thread
        """
        current_thread_id = task_id()
        session_id = self._get_session_id()

        # the order of the following checks is important
        self._verify_locked_session_id_if_present(current_thread_id, session_id, False)
        if session_id == _NO_SESSION_ID:
            self._lock_session_ids.pop(current_thread_id, None)
            raise self._new_illegal_monitor_state_error()

        try:
            still_locked_by_the_current_thread = await self._request_unlock(
                session_id, current_thread_id, uuid.uuid4()
            )
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

    async def is_locked(self) -> bool:
        """Returns whether this lock is locked or not.

        Returns:
            ``True`` if this lock is locked by any task in the cluster,
            ``False`` otherwise.

        Raises:
            LockOwnershipLostError: If the underlying CP session was
                closed before the client releases the lock
        """
        current_thread_id = task_id()
        session_id = self._get_session_id()
        self._verify_locked_session_id_if_present(current_thread_id, session_id, False)
        f = await self._request_get_lock_ownership_state()
        state = _LockOwnershipState(f)
        if state.is_locked_by(session_id, current_thread_id):
            self._lock_session_ids[current_thread_id] = session_id
            return True

        self._verify_no_locked_session_id_present(current_thread_id)
        return state.is_locked()

    async def is_locked_by_current_task(self) -> bool:
        """Returns whether the lock is held by the current task or not.

        Returns:
            ``True`` if the lock is held by the current task, ``False``
            otherwise.

        Raises:
            LockOwnershipLostError: If the underlying CP session was
                closed before the client releases the lock
        """
        current_thread_id = task_id()
        session_id = self._get_session_id()
        self._verify_locked_session_id_if_present(current_thread_id, session_id, False)
        f = await self._request_get_lock_ownership_state()
        state = _LockOwnershipState(f)
        locked_by_the_current_thread = state.is_locked_by(session_id, current_thread_id)
        if locked_by_the_current_thread:
            self._lock_session_ids[current_thread_id] = session_id
        else:
            self._verify_no_locked_session_id_present(current_thread_id)

        return locked_by_the_current_thread

    async def get_lock_count(self) -> int:
        """Returns the reentrant lock count if the lock is held by any task
        in the cluster.

        Returns:
            The reentrant lock count if the lock is held by any task in the
            cluster.

        Raises:
            LockOwnershipLostError: If the underlying CP session was
                closed before the client releases the lock
        """
        current_thread_id = task_id()
        session_id = self._get_session_id()
        self._verify_locked_session_id_if_present(current_thread_id, session_id, False)
        f = await self._request_get_lock_ownership_state()
        state = _LockOwnershipState(f)
        if state.is_locked_by(session_id, current_thread_id):
            self._lock_session_ids[current_thread_id] = session_id
        else:
            self._verify_no_locked_session_id_present(current_thread_id)

        return state.lock_count

    async def destroy(self) -> None:
        self._lock_session_ids.clear()
        return await super(FencedLock, self).destroy()

    async def _do_lock(self, current_thread_id, invocation_uuid):
        async def do_lock_once(session_id):
            self._verify_locked_session_id_if_present(current_thread_id, session_id, True)

            try:
                fence = await self._request_lock(session_id, current_thread_id, invocation_uuid)
            except SessionExpiredError:
                self._invalidate_session(session_id)
                self._verify_no_locked_session_id_present(current_thread_id)
                return await self._do_lock(current_thread_id, invocation_uuid)
            except WaitKeyCancelledError:
                self._release_session(session_id)
                raise IllegalMonitorStateError(
                    "Lock(%s) not acquired because the lock call on the CP group "
                    "is cancelled, possibly because of another indeterminate call "
                    "from the same task." % self._object_name
                )
            except Exception as e:
                self._release_session(session_id)
                raise e

            if fence != self.INVALID_FENCE:
                self._lock_session_ids[current_thread_id] = session_id
                return fence

            self._release_session(session_id)
            raise LockAcquireLimitReachedError(
                "Lock(%s) reentrant lock limit is already reached!" % self._object_name
            )

        session_id = await self._acquire_session()
        return await do_lock_once(session_id)

    async def _do_try_lock(self, current_thread_id, invocation_uuid, timeout):
        start = time.time()

        async def do_try_lock_once(session_id):
            self._verify_locked_session_id_if_present(current_thread_id, session_id, True)

            try:
                fence = await self._request_try_lock(
                    session_id, current_thread_id, invocation_uuid, timeout
                )
            except SessionExpiredError:
                self._invalidate_session(session_id)
                self._verify_no_locked_session_id_present(current_thread_id)
                remaining_timeout = timeout - (time.time() - start)
                if remaining_timeout <= 0:
                    return self.INVALID_FENCE
                return await self._do_try_lock(
                    current_thread_id, invocation_uuid, remaining_timeout
                )
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

        session_id = await self._acquire_session()
        return await do_try_lock_once(session_id)

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
        return LockOwnershipLostError(
            "Current task is not the owner of the Lock(%s) because its "
            "Session(%s) is closed by the server." % (self._proxy_name, lock_session_id)
        )

    def _new_illegal_monitor_state_error(self):
        return IllegalMonitorStateError(
            "Current task is not the owner of the Lock(%s)" % self._proxy_name
        )

    async def _request_lock(self, session_id, current_thread_id, invocation_uuid):
        codec = fenced_lock_lock_codec
        request = codec.encode_request(
            self._group_id, self._object_name, session_id, current_thread_id, invocation_uuid
        )
        return await self._ainvoke(request, codec.decode_response)

    async def _request_try_lock(self, session_id, current_thread_id, invocation_uuid, timeout):
        codec = fenced_lock_try_lock_codec
        request = codec.encode_request(
            self._group_id,
            self._object_name,
            session_id,
            current_thread_id,
            invocation_uuid,
            to_millis(timeout),
        )
        return await self._ainvoke(request, codec.decode_response)

    async def _request_unlock(self, session_id, current_thread_id, invocation_uuid):
        codec = fenced_lock_unlock_codec
        request = codec.encode_request(
            self._group_id, self._object_name, session_id, current_thread_id, invocation_uuid
        )
        return await self._ainvoke(request, codec.decode_response)

    async def _request_get_lock_ownership_state(self):
        codec = fenced_lock_get_lock_ownership_codec
        request = codec.encode_request(self._group_id, self._object_name)
        return await self._ainvoke(request, codec.decode_response)
