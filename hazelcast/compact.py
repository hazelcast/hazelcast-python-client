import logging
import typing

from hazelcast.errors import HazelcastSerializationError, IllegalStateError
from hazelcast.future import Future, ImmediateFuture
from hazelcast.invocation import Invocation
from hazelcast.protocol.codec import (
    client_fetch_schema_codec,
    client_send_schema_codec,
    client_send_all_schemas_codec,
)

if typing.TYPE_CHECKING:
    from hazelcast.cluster import ClusterService
    from hazelcast.config import Config
    from hazelcast.invocation import InvocationService
    from hazelcast.protocol.client_message import OutboundMessage
    from hazelcast.reactor import AsyncoreReactor
    from hazelcast.serialization.compact import (
        CompactStreamSerializer,
        Schema,
        SchemaNotReplicatedError,
    )

_logger = logging.getLogger(__name__)


class CompactSchemaService:
    _SEND_SCHEMA_RETRY_COUNT = 100

    def __init__(
        self,
        compact_serializer: "CompactStreamSerializer",
        invocation_service: "InvocationService",
        cluster_service: "ClusterService",
        reactor: "AsyncoreReactor",
        config: "Config",
    ):
        self._compact_serializer = compact_serializer
        self._invocation_service = invocation_service
        self._cluster_service = cluster_service
        self._reactor = reactor
        self._invocation_retry_pause = config.invocation_retry_pause
        self._has_replicated_schemas = False

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
        error: "SchemaNotReplicatedError",
        func: typing.Callable[..., Future],
        *args: typing.Any,
        **kwargs: typing.Any,
    ) -> Future:
        schema = error.schema
        clazz = error.clazz
        request = client_send_schema_codec.encode_request(schema)

        def callback():
            self._has_replicated_schemas = True
            self._compact_serializer.register_schema_to_type(schema, clazz)
            return func(*args, **kwargs)

        return self._replicate_schema(
            schema, request, CompactSchemaService._SEND_SCHEMA_RETRY_COUNT, callback
        )

    def _replicate_schema(
        self,
        schema: "Schema",
        request: "OutboundMessage",
        remaining_retries: int,
        callback: typing.Callable[..., Future],
    ) -> Future:
        def continuation(future: Future):
            replicated_members = future.result()
            members = self._cluster_service.get_members()
            for member in members:
                if member.uuid not in replicated_members:
                    break
            else:
                # Loop completed normally.
                # All members in our member list all known to have the schema
                return callback()

            # There is a member in our member list that the schema
            # is not known to be replicated yet. We should retry
            # sending it in a random member.
            if remaining_retries <= 1:
                # We tried to send it a couple of times, but the member list
                # in our local and the member list returned by the initiator
                # nodes did not match.
                raise IllegalStateError(
                    f"The schema {schema} cannot be replicated in the cluster, "
                    f"after {CompactSchemaService._SEND_SCHEMA_RETRY_COUNT} retries. "
                    f"It might be the case that the client is connected to the two "
                    f"halves of the cluster that is experiencing a split-brain, "
                    f"and continue putting the data associated with that schema "
                    f"might result in data loss. It might be possible to replicate "
                    f"the schema after some time, when the cluster is healed."
                )

            delayed_future: Future = Future()
            self._reactor.add_timer(
                self._invocation_retry_pause,
                lambda: delayed_future.set_result(None),
            )

            def retry(_):
                return self._replicate_schema(
                    schema, request.copy(), remaining_retries - 1, callback
                )

            return delayed_future.continue_with(retry)

        return self._send_schema_replication_request(request).continue_with(continuation)

    def _send_schema_replication_request(self, request: "OutboundMessage") -> Future:
        invocation = Invocation(request, response_handler=client_send_schema_codec.decode_response)
        self._invocation_service.invoke(invocation)
        return invocation.future

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

    def register_fetched_schema(self, schema_id: int, schema: typing.Optional["Schema"]) -> None:
        if not schema:
            raise HazelcastSerializationError(
                f"The schema with the id {schema_id} can not be found in the cluster."
            )

        self._compact_serializer.register_schema_to_id(schema)

    def has_replicated_schemas(self):
        """
        Returns ``True`` is the client has replicated
        any Compact schemas to the cluster.
        """
        return self._has_replicated_schemas
