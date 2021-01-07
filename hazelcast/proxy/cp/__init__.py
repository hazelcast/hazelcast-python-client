from hazelcast.future import make_blocking
from hazelcast.invocation import Invocation
from hazelcast.protocol.codec import cp_group_destroy_cp_object_codec


def _no_op_response_handler(_):
    return None


class BaseCPProxy(object):
    def __init__(self, context, group_id, service_name, proxy_name, object_name):
        self._context = context
        self._group_id = group_id
        self._service_name = service_name
        self._proxy_name = proxy_name
        self._object_name = object_name
        self._invocation_service = context.invocation_service
        serialization_service = context.serialization_service
        self._to_data = serialization_service.to_data
        self._to_object = serialization_service.to_object

    def destroy(self):
        """Destroys this proxy."""
        codec = cp_group_destroy_cp_object_codec
        request = codec.encode_request(self._group_id, self._service_name, self._object_name)
        return self._invoke(request)

    def blocking(self):
        """Returns a version of this proxy with only blocking method calls."""
        return make_blocking(self)

    def _invoke(self, request, response_handler=_no_op_response_handler):
        invocation = Invocation(request, response_handler=response_handler)
        self._invocation_service.invoke(invocation)
        return invocation.future


class SessionAwareCPProxy(BaseCPProxy):
    def __init__(self, context, group_id, service_name, proxy_name, object_name):
        super(SessionAwareCPProxy, self).__init__(
            context, group_id, service_name, proxy_name, object_name
        )
        self._session_manager = context.proxy_session_manager

    def get_group_id(self):
        """
        Returns:
           hazelcast.protocol.RaftGroupId: Id of the CP group that runs this proxy.
        """
        return self._group_id

    def _get_session_id(self):
        """
        Returns:
            int: Session id.
        """
        return self._session_manager.get_session_id(self._group_id)

    def _acquire_session(self, count=1):
        """
        Returns:
            hazelcast.future.Future[int]: Session id.
        """
        return self._session_manager.acquire_session(self._group_id, count)

    def _release_session(self, session_id, count=1):
        self._session_manager.release_session(self._group_id, session_id, count)

    def _invalidate_session(self, session_id):
        self._session_manager.invalidate_session(self._group_id, session_id)
