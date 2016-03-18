import logging
import threading
from hazelcast.hash import hash_to_index
from hazelcast.protocol.codec import client_get_partitions_codec

PARTITION_UPDATE_INTERVAL = 10


class PartitionService(object):
    logger = logging.getLogger("PartitionService")
    timer = None

    def __init__(self, client):
        self.partitions = {}
        self._client = client

    def start(self):
        self.logger.debug("Starting partition service")

        def partition_updater():
            self._do_refresh()
            self.timer = self._client.reactor.add_timer(PARTITION_UPDATE_INTERVAL, partition_updater)

        self.timer = self._client.reactor.add_timer(PARTITION_UPDATE_INTERVAL, partition_updater)

    def shutdown(self):
        if self.timer:
            self.timer.cancel()

    def refresh(self):
        self._client.reactor.add_timer(0, self._do_refresh)

    def get_partition_owner(self, partition_id):
        if partition_id not in self.partitions:
            self._do_refresh()
        return self.partitions[partition_id]

    def get_partition_id(self, key):
        data = self._client.serialization_service.to_data(key)
        count = self.get_partition_count()
        return hash_to_index(data.get_partition_hash(), count)

    def get_partition_count(self):
        if not self.partitions:
            self._get_partition_count_blocking()
        return len(self.partitions)

    def _get_partition_count_blocking(self):
        event = threading.Event()
        while not self.partitions:
            self._do_refresh(callback=lambda: event.set())
            event.wait(timeout=1)

    def _do_refresh(self, callback=None):
        self.logger.debug("Start updating partitions")
        address = self._client.cluster.owner_connection_address
        connection = self._client.connection_manager.get_connection(address)
        if connection is None:
            self.logger.debug("Could not update partition thread as owner connection is not available.")
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
        # TODO: needs sync
        self.partitions = {}
        for addr, partition_list in partitions.iteritems():
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
