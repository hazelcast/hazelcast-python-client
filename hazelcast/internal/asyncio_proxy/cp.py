from hazelcast.internal.asyncio_invocation import Invocation
from hazelcast.protocol.codec import cp_group_destroy_cp_object_codec


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
