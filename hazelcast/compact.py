import logging
import typing

from hazelcast.errors import HazelcastSerializationError
from hazelcast.future import Future, ImmediateFuture
from hazelcast.invocation import InvocationService, Invocation
from hazelcast.protocol.codec import (
    client_fetch_schema_codec,
    client_send_schema_codec,
    client_send_all_schemas_codec,
)
from hazelcast.serialization.compact import (
    CompactStreamSerializer,
    Schema,
    SchemaNotReplicatedError,
)

_logger = logging.getLogger(__name__)


class CompactSchemaService:
    def __init__(
        self,
        compact_serializer: CompactStreamSerializer,
        invocation_service: InvocationService,
    ):
        self._compact_serializer = compact_serializer
        self._invocation_service = invocation_service

    def fetch_schema(self, schema_id: int) -> Future:
        _logger.debug(
            "Could not find schema with the id %s locally. It will be fetched from the cluster.",
            schema_id,
        )

        request = client_fetch_schema_codec.encode_request(schema_id)
        fetch_schema_invocation = Invocation(
            request,
            response_handler=client_fetch_schema_codec.decode_response,
        )
        self._invocation_service.invoke(fetch_schema_invocation)
        return fetch_schema_invocation.future

    def send_schema_and_retry(
        self,
        error: SchemaNotReplicatedError,
        func: typing.Callable[..., Future],
        *args: typing.Any,
        **kwargs: typing.Any,
    ) -> Future:
        schema = error.schema
        clazz = error.clazz
        request = client_send_schema_codec.encode_request(schema)
        invocation = Invocation(request)

        def continuation(future):
            future.result()
            self._compact_serializer.register_schema_to_type(schema, clazz)
            return func(*args, **kwargs)

        self._invocation_service.invoke(invocation)
        return invocation.future.continue_with(continuation)

    def send_all_schemas(self) -> Future:
        schemas = self._compact_serializer.get_schemas()
        if not schemas:
            _logger.debug("There is no schema to send to the cluster.")
            return ImmediateFuture(None)

        _logger.debug("Sending the following schemas to the cluster: %s", schemas)

        request = client_send_all_schemas_codec.encode_request(schemas)
        invocation = Invocation(request, urgent=True)
        self._invocation_service.invoke(invocation)
        return invocation.future

    def register_fetched_schema(self, schema_id: int, schema: typing.Optional[Schema]) -> None:
        if not schema:
            raise HazelcastSerializationError(
                f"The schema with the id {schema_id} can not be found in the cluster."
            )

        self._compact_serializer.register_schema_to_id(schema)
