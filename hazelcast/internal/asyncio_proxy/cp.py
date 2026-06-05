import abc
import asyncio

from hazelcast.cp import _SessionState
from hazelcast.errors import (
    HazelcastClientNotActiveError,
    SessionExpiredError,
    CPGroupDestroyedError,
)
from hazelcast.internal.asyncio_invocation import Invocation
from hazelcast.protocol import RaftGroupId
from hazelcast.protocol.codec import (
    cp_group_destroy_cp_object_codec,
    cp_session_generate_thread_id_codec,
    cp_session_close_session_codec,
    cp_session_create_session_codec,
    cp_session_heartbeat_session_codec,
)


def _no_op_response_handler(_):
    return None


class BaseCPProxy:
    def __init__(self, context, group_id, service_name, proxy_name, object_name):
        self._group_id = group_id
        self._service_name = service_name
        self._proxy_name = proxy_name
        self._object_name = object_name
        self._invocation_service = context.invocation_service
        serialization_service = context.serialization_service
        self._to_data = serialization_service.to_data
        self._to_object = serialization_service.to_object
        self._send_schema_and_retry = context.compact_schema_service.send_schema_and_retry

    async def destroy(self) -> None:
        """Destroys this proxy."""
        codec = cp_group_destroy_cp_object_codec
        request = codec.encode_request(self._group_id, self._service_name, self._object_name)
        return await self._ainvoke(request)

    def _invoke(self, request, response_handler=_no_op_response_handler):
        invocation = Invocation(request, response_handler=response_handler)
        self._invocation_service.invoke(invocation)
        return invocation.future

    async def _ainvoke(self, request, response_handler=_no_op_response_handler):
        fut = self._invoke(request, response_handler)
        return await fut


class SessionAwareCPProxy(BaseCPProxy, abc.ABC):
    def __init__(self, context, group_id, service_name, proxy_name, object_name):
        super(SessionAwareCPProxy, self).__init__(
            context, group_id, service_name, proxy_name, object_name
        )
        self._session_manager = context.proxy_session_manager

    def get_group_id(self) -> RaftGroupId:
        """
        Returns:
           Id of the CP group that runs this proxy.
        """
        return self._group_id

    def _get_session_id(self) -> int:
        return self._session_manager.get_session_id(self._group_id)

    async def _acquire_session(self, count: int = 1) -> int:
        return await self._session_manager.acquire_session(self._group_id, count)

    def _release_session(self, session_id: int, count: int = 1) -> None:
        self._session_manager.release_session(self._group_id, session_id, count)

    def _invalidate_session(self, session_id: int) -> None:
        self._session_manager.invalidate_session(self._group_id, session_id)


_NO_SESSION_ID = -1


class ProxySessionManager:
    def __init__(self, context):
        self._context = context
        self._mutexes = dict()  # RaftGroupId to asyncio.Lock
        self._sessions = dict()  # RaftGroupId to SessionState
        self._thread_ids = dict()  # (RaftGroupId, thread_id) to global thread id
        self._heartbeat_task = None
        self._shutdown = False
        self._lock = asyncio.Lock()

    def get_session_id(self, group_id):
        session = self._sessions.get(group_id, None)
        if session is None:
            return _NO_SESSION_ID
        return session.id

    async def acquire_session(self, group_id, count):
        state = await self._get_or_create_session(group_id)
        return state.acquire(count)

    def release_session(self, group_id, session_id, count):
        session = self._sessions.get(group_id, None)
        if session and session.id == session_id:
            session.release(count)

    def invalidate_session(self, group_id, session_id):
        session = self._sessions.get(group_id, None)
        if session and session.id == session_id:
            self._sessions.pop(group_id, None)

    async def get_or_create_unique_thread_id(self, group_id):
        async with self._lock:
            if self._shutdown:
                raise HazelcastClientNotActiveError("Session manager is already shut down!")

            # TODO: replace 0 with the lock context once implemented
            key = (group_id, 0)
            global_thread_id = self._thread_ids.get(key)
            if global_thread_id:
                return global_thread_id

            tid = await self._request_generate_thread_id(group_id)
            return self._thread_ids.setdefault(key, tid)

    async def shutdown(self):
        async with self._lock:
            if self._shutdown:
                return None

            self._shutdown = True
            if self._heartbeat_task:
                self._heartbeat_task.cancel()

            tasks = []
            async with asyncio.TaskGroup() as tg:
                for session in list(self._sessions.values()):
                    tasks.append(
                        tg.create_task(self._request_close_session(session.group_id, session.id))
                    )

            self._sessions.clear()
            self._mutexes.clear()
            self._thread_ids.clear()

    async def _request_generate_thread_id(self, group_id):
        codec = cp_session_generate_thread_id_codec
        request = codec.encode_request(group_id)
        invocation = Invocation(request, response_handler=codec.decode_response)
        return await self._context.invocation_service.ainvoke(invocation)

    async def _request_close_session(self, group_id, session_id):
        codec = cp_session_close_session_codec
        request = codec.encode_request(group_id, session_id)
        invocation = Invocation(request, response_handler=codec.decode_response)
        return await self._context.invocation_service.ainvoke(invocation)

    async def _get_or_create_session(self, group_id):
        async with self._lock:
            if self._shutdown:
                raise HazelcastClientNotActiveError("Session manager is already shut down!")

            session = self._sessions.get(group_id, None)
            if session is None or not session.is_valid():
                async with self._mutex(group_id):
                    session = self._sessions.get(group_id)
                    if session is None or not session.is_valid():
                        return await self._create_new_session(group_id)
            return session

    async def _create_new_session(self, group_id):
        response = await self._request_new_session(group_id)
        return self._do_create_new_session(response, group_id)

    def _do_create_new_session(self, response, group_id):
        session = _SessionState(response["session_id"], group_id, response["ttl_millis"] / 1000.0)
        self._sessions[group_id] = session
        self._start_heartbeat_timer(response["heartbeat_millis"] / 1000.0)
        return session

    async def _request_new_session(self, group_id):
        codec = cp_session_create_session_codec
        request = codec.encode_request(group_id, self._context.name)
        invocation = Invocation(request, response_handler=codec.decode_response)
        return await self._context.invocation_service.ainvoke(invocation)

    def _mutex(self, group_id) -> asyncio.Lock:
        mutex = self._mutexes.get(group_id, None)
        if mutex is not None:
            return mutex

        mutex = asyncio.Lock()
        current = self._mutexes.setdefault(group_id, mutex)
        return current

    def _start_heartbeat_timer(self, period):
        if self._heartbeat_task is not None:
            return

        async def heartbeat():
            await asyncio.sleep(period)
            if self._shutdown:
                return

            for session in list(self._sessions.values()):
                if session.is_in_use():

                    def cb(heartbeat_future: asyncio.Future, session=session):
                        error = heartbeat_future.exception()
                        if error is None:
                            return

                        if isinstance(error, (SessionExpiredError, CPGroupDestroyedError)):
                            self.invalidate_session(session.group_id, session.id)

                    f = self._request_heartbeat(session.group_id, session.id)
                    f.add_done_callback(cb)

            self._heartbeat_task = asyncio.create_task(heartbeat())

        self._heartbeat_task = asyncio.create_task(heartbeat())

    def _request_heartbeat(self, group_id, session_id) -> asyncio.Future:
        codec = cp_session_heartbeat_session_codec
        request = codec.encode_request(group_id, session_id)
        invocation = Invocation(request)
        self._context.invocation_service.invoke(invocation)
        return invocation.future
