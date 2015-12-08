import logging
import threading
from hazelcast.protocol.codec import client_get_partitions_codec


class PartitionService(object):
    logger = logging.getLogger("PartitionService")

    def __init__(self, client):
        self.partitions = {}
        self._client = client
        self.t = None

    def start(self):
        def partition_updater():
            self._do_refresh()
            self.t = self._schedule_refresh(10)

        self.t = threading.Timer(10, partition_updater)
        self.t.setDaemon(True)
        self.t.start()  # TODO: find a better scheduling option

    def refresh(self):
        self._schedule_refresh(0)

    def _schedule_refresh(self, delay):
        t = threading.Timer(delay, self._do_refresh)
        t.setDaemon(True)
        t.start()
        return t

    def get_partition_owner(self, partition_id):
        if partition_id not in self.partitions:
            self._do_refresh()
        return self.partitions[partition_id]

    def get_partition_id(self, data):
        count = self._get_partition_count()
        return data.get_partition_hash() % count

    def _get_partition_count(self):
        if len(self.partitions) == 0:
            self._do_refresh()

        return len(self.partitions)

    def _do_refresh(self):
        self.logger.debug("Start updating partitions")
        address = self._client.cluster.owner_connection_address
        connection = self._client.connection_manager.get_connection(address)
        if connection is None:
            self.logger.debug("Could not update partition thread as owner connection is not established yet.")
            return
        request = client_get_partitions_codec.encode_request()
        response = self._client.invoker.invoke_on_connection(request, connection).result()
        partitions = client_get_partitions_codec.decode_response(response)["partitions"]
        # TODO: needs sync
        self.partitions = {}
        for addr, partition_list in partitions.iteritems():
            for partition in partition_list:
                self.partitions[partition] = addr
        self.logger.debug("Finished updating partitions")
        # TODO: exception handling
