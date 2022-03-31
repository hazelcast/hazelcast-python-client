import abc
import typing

from hazelcast.future import Future
from hazelcast.invocation import Invocation
from hazelcast.protocol import RaftGroupId
from hazelcast.protocol.codec import cp_group_destroy_cp_object_codec
from hazelcast.types import BlockingProxyType


def _no_op_response_handler(_):
    return None


class BaseCPProxy(typing.Generic[BlockingProxyType], abc.ABC):
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

    def destroy(self) -> Future[None]:
        """Destroys this proxy."""
        codec = cp_group_destroy_cp_object_codec
        request = codec.encode_request(self._group_id, self._service_name, self._object_name)
        return self._invoke(request)

    @abc.abstractmethod
    def blocking(self) -> BlockingProxyType:
        """Returns a version of this proxy with only blocking method calls."""
        pass

    def _invoke(self, request, response_handler=_no_op_response_handler):
        invocation = Invocation(request, response_handler=response_handler)
        self._invocation_service.invoke(invocation)
        return invocation.future


class SessionAwareCPProxy(BaseCPProxy[BlockingProxyType], abc.ABC):
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
        """
        Returns:
            Session id.
        """
        return self._session_manager.get_session_id(self._group_id)

    def _acquire_session(self, count: int = 1) -> Future[int]:
        """
        Returns:
            Session id.
        """
        return self._session_manager.acquire_session(self._group_id, count)

    def _release_session(self, session_id: int, count: int = 1) -> None:
        self._session_manager.release_session(self._group_id, session_id, count)

    def _invalidate_session(self, session_id: int) -> None:
        self._session_manager.invalidate_session(self._group_id, session_id)
