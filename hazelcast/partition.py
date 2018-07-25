import logging
import threading
from hazelcast.hash import hash_to_index
from hazelcast.protocol.codec import client_get_partitions_codec
from hazelcast import six

PARTITION_UPDATE_INTERVAL = 10


class PartitionService(object):
    """
    An SPI service for accessing partition related information.
    """
    logger = logging.getLogger("PartitionService")
    timer = None

    def __init__(self, client):
        self.partitions = {}
        self._client = client

    def start(self):
        """
        Starts the partition service.
        """
        self.logger.debug("Starting partition service")

        def partition_updater():
            self._do_refresh()
            self.timer = self._client.reactor.add_timer(PARTITION_UPDATE_INTERVAL, partition_updater)

        self.timer = self._client.reactor.add_timer(PARTITION_UPDATE_INTERVAL, partition_updater)

    def shutdown(self):
        """
        Shutdowns the partition service.
        """
        if self.timer:
            self.timer.cancel()

    def refresh(self):
        """
        Refreshes the partition service.
        """
        self._client.reactor.add_timer(0, self._do_refresh)

    def get_partition_owner(self, partition_id):
        """
        Gets the owner of the partition if it's set. Otherwise it will trigger partition assignment.

        :param partition_id: (int), the partition id.
        :return: (:class:`~hazelcast.core.Address`), owner of partition or ``None`` if it's not set yet.
        """
        if partition_id not in self.partitions:
            self._do_refresh()
        return self.partitions.get(partition_id, None)

    def get_partition_id(self, key):
        """
        Returns the partition id for a Data key.

        :param key: (object), the data key.
        :return: (int), the partition id.
        """
        data = self._client.serialization_service.to_data(key)
        count = self.get_partition_count()
        if count <= 0:
            return 0
        return hash_to_index(data.get_partition_hash(), count)

    def get_partition_count(self):
        """
        Returns the number of partitions.

        :return: (int), the number of partitions.
        """
        if not self.partitions:
            self._get_partition_count_blocking()
        return len(self.partitions)

    def _get_partition_count_blocking(self):
        event = threading.Event()
        while not event.isSet():
            self._do_refresh(callback=lambda: event.set())
            event.wait(timeout=1)

    def _do_refresh(self, callback=None):
        self.logger.debug("Start updating partitions")
        address = self._client.cluster.owner_connection_address
        connection = self._client.connection_manager.get_connection(address)
        if connection is None:
            self.logger.debug("Could not update partition thread as owner connection is not available.")
            if callback:
                callback()
            return
        request = client_get_partitions_codec.encode_request()

        def cb(f):
            if f.is_success():
                self.process_partition_response(f.result())
                if callback:
                    callback()

        future = self._client.invoker.invoke_on_connection(request, connection)
        future.add_done_callback(cb)

    def process_partition_response(self, message):
        partitions = client_get_partitions_codec.decode_response(message)["partitions"]
        for addr, partition_list in six.iteritems(partitions):
            for partition in partition_list:
                self.partitions[partition] = addr
        self.logger.debug("Finished updating partitions")


def string_partition_strategy(key):
    if key is None:
        return None
    try:
        index_of = key.index('@')
        return key[index_of + 1:]
    except ValueError:
        return key
