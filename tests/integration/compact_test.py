import typing

from mock.mock import patch

import hazelcast
from hazelcast.compact import CompactSchemaService
from hazelcast.errors import IllegalStateError
from hazelcast.future import ImmediateFuture
from hazelcast.serialization.api import CompactSerializer, CompactWriter, CompactReader
from tests.base import HazelcastTestCase
from tests.util import random_string


# The tests under this file are meant to be not
# run with the backward compatibility tests, as
# they test implementation details through private
# APIs. If you are going to add a test to this file,
# make sure that it fits the description above. If not,
# add it to tests under the backward_compatible folder.


class CompactSchemaReplicationRetryTest(HazelcastTestCase):
    rc = None
    cluster = None
    retry_count = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.rc = cls.create_rc()
        cls.cluster = cls.create_cluster(cls.rc, None)
        cls.cluster.start_member()
        cls.cluster.start_member()
        cls.retry_count = CompactSchemaService._SEND_SCHEMA_RETRY_COUNT
        CompactSchemaService._SEND_SCHEMA_RETRY_COUNT = 10

    def setUp(self) -> None:
        self.client = hazelcast.HazelcastClient(
            compact_serializers=[FooSerializer()],
            cluster_name=self.cluster.id,
            invocation_retry_pause=0.1,
        )
        self.map = self.client.get_map(random_string()).blocking()

    def tearDown(self) -> None:
        self.client.shutdown()

    @classmethod
    def tearDownClass(cls):
        CompactSchemaService._SEND_SCHEMA_RETRY_COUNT = cls.retry_count
        cls.rc.terminateCluster(cls.cluster.id)
        cls.rc.exit()

    def test_compact_schemas_replicated(self):
        schema_service = self.client._compact_schema_service
        with patch.object(
            schema_service,
            "_send_schema_replication_request",
            wraps=schema_service._send_schema_replication_request,
        ) as wrapped_send:
            self.map.put(1, Foo(1))
            wrapped_send.assert_called_once()

    def test_compact_schema_replication_retried_when_a_schema_is_not_replicated_to_all_members(
        self,
    ):
        schema_service = self.client._compact_schema_service
        members = self.client.cluster_service.get_members()
        self.assertEqual(2, len(members))

        with patch.object(
            schema_service,
            "_send_schema_replication_request",
            return_value=ImmediateFuture(
                {members[0].uuid}
            ),  # Return a single member uuid all the time
        ) as wrapped_send:
            with self.assertRaisesRegex(
                IllegalStateError, "connected to the two halves of the cluster"
            ):
                self.map.put(1, Foo(1))

            self.assertEqual(10, wrapped_send.call_count)

    def test_compact_schema_replication_succeeds_after_some_failed_attempts(self):
        schema_service = self.client._compact_schema_service
        members = self.client.cluster_service.get_members()
        self.assertEqual(2, len(members))

        with patch.object(
            schema_service,
            "_send_schema_replication_request",
            side_effect=[
                # Return a single member uuid for two times
                ImmediateFuture({members[0].uuid}),
                ImmediateFuture({members[0].uuid}),
                # Return all member uuids for the third time
                ImmediateFuture({members[0].uuid, members[1].uuid}),
            ],
        ) as wrapped_send:
            self.map.put(1, Foo(1))
            self.assertEqual(3, wrapped_send.call_count)


class Foo:
    def __init__(self, bar: int):
        self.bar = bar


class FooSerializer(CompactSerializer[Foo]):
    def read(self, reader: CompactReader) -> Foo:
        return Foo(reader.read_int32("bar"))

    def write(self, writer: CompactWriter, obj: Foo) -> None:
        writer.write_int32("bar", obj.bar)

    def get_class(self) -> typing.Type[Foo]:
        return Foo

    def get_type_name(self) -> str:
        return "Foo"
