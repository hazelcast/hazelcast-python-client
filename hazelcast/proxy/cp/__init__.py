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
