import time
import uuid

from hazelcast.errors import (
    LockOwnershipLostError,
    IllegalMonitorStateError,
    SessionExpiredError,
    WaitKeyCancelledError,
    LockAcquireLimitReachedError,
)
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
        # TODO: port the docs once implemented
        # TODO: replace 0 with the lock context once implemented
        current_thread_id = 0
        invocation_uuid = uuid.uuid4()
        return await self._do_lock(current_thread_id, invocation_uuid)

    async def try_lock(self, timeout: float = 0) -> int:
        # TODO: port the docs once implemented
        # TODO: replace 0 with the lock context once implemented
        current_thread_id = 0
        invocation_uuid = uuid.uuid4()
        timeout = max(0.0, timeout)
        return await self._do_try_lock(current_thread_id, invocation_uuid, timeout)

    async def unlock(self) -> None:
        # TODO: port the docs once implemented
        # TODO: replace 0 with the lock context once implemented
        current_thread_id = 0
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
        # TODO: port the docs once implemented
        # TODO: replace 0 with the lock context once implemented
        current_thread_id = 0
        session_id = self._get_session_id()
        self._verify_locked_session_id_if_present(current_thread_id, session_id, False)
        f = await self._request_get_lock_ownership_state()
        state = _LockOwnershipState(f)
        if state.is_locked_by(session_id, current_thread_id):
            self._lock_session_ids[current_thread_id] = session_id
            return True

        self._verify_no_locked_session_id_present(current_thread_id)
        return state.is_locked()

    async def is_locked_by_current_thread(self) -> bool:
        # TODO: port the docs once implemented
        # TODO: replace 0 with the lock context once implemented
        current_thread_id = 0
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
        # TODO: port the docs once implemented
        # TODO: replace 0 with the lock context once implemented
        current_thread_id = 0
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
                    "from the same thread." % self._object_name
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
            "Current thread is not the owner of the Lock(%s) because its "
            "Session(%s) is closed by the server." % (self._proxy_name, lock_session_id)
        )

    def _new_illegal_monitor_state_error(self):
        return IllegalMonitorStateError(
            "Current thread is not the owner of the Lock(%s)" % self._proxy_name
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

