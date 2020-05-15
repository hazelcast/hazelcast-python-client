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
    logger = logging.getLogger("HazelcastClient.PartitionService")
    timer = None

    def __init__(self, client):
        self._client = client
        self._logger_extras = {"client_name": client.name, "group_name": client.config.group_config.name}
        self.partition_count = 0
        self.partition_table = PartitionTable(None, -1, {})

    def start(self):
        """
        Starts the partition service.
        """
        self.logger.debug("Starting partition service", extra=self._logger_extras)

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
        Gets the owner of the partition if it's set. Otherwise it returns None.

        :param partition_id: (int), the partition id.
        :return: (:class:`uuid.UUID`), owner of partition or ``None`` if it's not set yet.
        """
        #print("get partition owner {}".format(partition_id not in self.partition_table.partitions))
        if not self.partition_table.partitions or partition_id not in list(self.partition_table.partitions.values())[0]:
            return None
        for key, value in self.partition_table.partitions.items():
            if partition_id in value:
                #print("key:{}".format(key))
                return key
        #return self.partition_table.partitions.get(partition_id, None)

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
        return self.partition_count

    def reset(self):
        """
        Resets the partition table to initial state.
        """
        self.partition_table.connection = None
        self.partition_table.partition_state_version = -1
        self.partition_table.partitions = {}

    def process_partition_response(self, message):
        partitions = client_get_partitions_codec.decode_response(message)["partitions"]
        partitions_dict = {}
        for addr, partition_list in six.iteritems(partitions):
            for partition in partition_list:
                partitions_dict[partition] = addr
        self.partition_table.partitions.update(partitions_dict)
        self.logger.debug("Finished updating partitions", extra=self._logger_extras)

    def handle_partitions_view_event(self, connection, partitions, partition_state_version):

        logging.debug("Handling new partition table with  partitionStateVersion: {}".format(partition_state_version),
                      extra=self._logger_extras)

        while True:
            if not self.should_be_applied(connection, partitions, partition_state_version, self.partition_table):
                return
            if isinstance(partitions, list):
                dict_partitions = {}
                for i, j in partitions:
                    dict_partitions[i] = j
                self.partition_table.connection = connection
                self.partition_table.partition_state_version = partition_state_version
                self.partition_table.partitions = dict_partitions
                # self.partition_table = PartitionTable(connection, partition_state_version, dict_partitions)
            else:
                self.partition_table.connection = connection
                self.partition_table.partition_state_version = partition_state_version
                self.partition_table.partitions = partitions
                # self.partition_table = PartitionTable(connection, partition_state_version, partitions)
            logging.debug("Applied partition table with partitionStateVersion : " + str(partition_state_version),
                          extra=self._logger_extras)

    def should_be_applied(self, connection, partitions, partition_state_version, current):
        if not partitions:
            logging.warning(connection, partition_state_version, current,
                            "response is empty", extra=self._logger_extras)
            return False
        if not current:
            logging.warning("Event coming from a new connection. Old connection: "
                            + ", new connection ", extra=self._logger_extras)
            return True
        if connection is not current.connection:
            logging.warning("Event coming from a new connection. Old connection: " + str(current.connection)
                            + ", new connection " + str(connection), extra=self._logger_extras)
            return True
        if partition_state_version <= current.partition_state_version:
            logging.debug("{} {} response state version is old".format(partition_state_version,  current.partition_state_version), extra=self._logger_extras)
            return False

        return True

    def check_and_set_partition_count(self, new_partition_count):
        if self.partition_count == 0:
            self.partition_count = new_partition_count
            return True

        return self.partition_count == new_partition_count

    def _do_refresh(self):
        pass


def string_partition_strategy(key):
    if key is None:
        return None
    try:
        index_of = key.index('@')
        return key[index_of + 1:]
    except ValueError:
        return key


class PartitionTable(object):
    def __init__(self, connection, partition_state_version, partitions):
        self.partitions = partitions
        self.partition_state_version = partition_state_version
        self.connection = connection
