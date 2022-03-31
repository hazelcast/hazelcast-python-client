import logging
import uuid

import typing

from hazelcast.errors import ClientOfflineError
from hazelcast.hash import hash_to_index
from hazelcast.serialization.compact import SchemaNotReplicatedError

_logger = logging.getLogger(__name__)


class _PartitionTable:
    __slots__ = ("connection", "version", "partitions")

    def __init__(self, connection, version, partitions):
        self.connection = connection
        self.version = version
        self.partitions = partitions

    def __repr__(self):
        return "PartitionTable(connection=%s, version=%s)" % (self.connection, self.version)


class PartitionService:
    """
    Allows retrieving information about the partition count, the partition
    owner or the partition id of a key.
    """

    __slots__ = ("_service", "_serialization_service", "_send_schema_and_retry_fn")

    def __init__(self, internal_partition_service, serialization_service, send_schema_and_retry_fn):
        self._service = internal_partition_service
        self._serialization_service = serialization_service
        self._send_schema_and_retry_fn = send_schema_and_retry_fn

    def get_partition_owner(self, partition_id: int) -> typing.Optional[uuid.UUID]:
        """
        Returns the owner of the partition if it's set, ``None`` otherwise.

        Args:
            partition_id: The partition id.

        Returns:
            Owner of the partition
        """
        return self._service.get_partition_owner(partition_id)

    def get_partition_id(self, key: typing.Any) -> int:
        """
        Returns the partition id for a key data.

        Args:
            key: The given key.

        Returns:
            The partition id.
        """
        try:
            key_data = self._serialization_service.to_data(key)
        except SchemaNotReplicatedError as e:
            self._send_schema_and_retry_fn(e, lambda: None).result()
            return self.get_partition_id(key)

        return self._service.get_partition_id(key_data)

    def get_partition_count(self) -> int:
        """
        Returns partition count of the connected cluster.

        If partition table is not fetched yet, this method returns ``0``.

        Returns:
            The partition count
        """
        return self._service.partition_count


class _InternalPartitionService:
    __slots__ = ("partition_count", "_client", "_partition_table")

    def __init__(self, client):
        self.partition_count = 0
        self._client = client
        self._partition_table = _PartitionTable(None, -1, {})

    def handle_partitions_view_event(self, connection, partitions, version):
        _logger.debug("Handling new partition table with version: %s", version)

        table = self._partition_table
        if not self._should_be_applied(connection, partitions, version, table):
            return

        new_partitions = self._prepare_partitions(partitions)
        new_table = _PartitionTable(connection, version, new_partitions)
        self._partition_table = new_table

    def get_partition_owner(self, partition_id):
        return self._partition_table.partitions.get(partition_id, None)

    def get_partition_id(self, key):
        if self.partition_count == 0:
            # Partition count can not be zero for the SYNC mode.
            # On the SYNC mode, we are waiting for the first connection to be established.
            # We are initializing the partition count with the value coming from the server with authentication.
            # This error is used only for ASYNC mode client.
            raise ClientOfflineError()
        return hash_to_index(key.get_partition_hash(), self.partition_count)

    def check_and_set_partition_count(self, partition_count):
        if self.partition_count == 0:
            self.partition_count = partition_count
            return True
        return self.partition_count == partition_count

    @classmethod
    def _should_be_applied(cls, connection, partitions, version, current):
        if not partitions:
            _logger.debug(
                "Partition view will not be applied since response is empty. "
                "Sending connection: %s, version: %s, current table: %s",
                connection,
                version,
                current,
            )
            return False

        if connection != current.connection:
            _logger.debug(
                "Partition view event coming from a new connection. Old: %s, new: %s",
                current.connection,
                connection,
            )
            return True

        if version <= current.version:
            _logger.debug(
                "Partition view will not be applied since response state version is older. "
                "Sending connection: %s, version: %s, current table: %s",
                connection,
                version,
                current,
            )
            return False

        return True

    @classmethod
    def _prepare_partitions(cls, partitions):
        new_partitions = {}
        for uuid, partition_list in partitions:
            for partition in partition_list:
                new_partitions[partition] = uuid
        return new_partitions


def string_partition_strategy(key):
    if key is None:
        return None
    try:
        index_of = key.index("@")
        return key[index_of + 1 :]
    except ValueError:
        return key
