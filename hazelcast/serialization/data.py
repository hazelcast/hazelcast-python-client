from struct import unpack_from

from hazelcast.hash import murmur_hash3_x86_32
from hazelcast.serialization.bits import *
from hazelcast.serialization.serialization_const import *

PARTITION_HASH_OFFSET = 0
TYPE_OFFSET = 4
DATA_OFFSET = 8

HEAP_DATA_OVERHEAD = DATA_OFFSET


class Data(object):
    """
    Data is basic unit of serialization. It stores binary form of an object serialized by serialization service
    """

    def __init__(self, buff=None):
        self._buffer = buff

    def to_bytes(self):
        """
        Returns byte array representation of internal binary format.

        :return:  (byte array), byte array representation of internal binary format.
        """
        return self._buffer

    def get_type(self):
        """
        Returns serialization type of binary form.

        :return: Serialization type of binary form.
        """
        if self.total_size() == 0:
            return CONSTANT_TYPE_NULL
        return unpack_from(FMT_BE_INT, self._buffer, TYPE_OFFSET)[0]

    def total_size(self):
        """
        Returns the total size of Data in bytes.

        :return: (int), total size of Data in bytes.
        """
        return len(self._buffer) if self._buffer is not None else 0

    def data_size(self):
        """
        Returns size of internal binary data in bytes.

        :return: (int), size of internal binary data in bytes.
        """
        return max(self.total_size() - HEAP_DATA_OVERHEAD, 0)

    def get_partition_hash(self):
        """
        Returns partition hash calculated for serialized object.
            Partition hash is used to determine partition of a Data and is calculated using
                * PartitioningStrategy during serialization.
                * If partition hash is not set then hash_code() is used.

        :return: partition hash
        """
        if self.has_partition_hash():
            return unpack_from(FMT_BE_INT, self._buffer, PARTITION_HASH_OFFSET)[0]
        return self.hash_code()

    def is_portable(self):
        """
        Determines whether this Data is created from a :class:`~hazelcast.serialization.api.Portable`. object or not.

        :return: (bool), ``true`` if source object is Portable, ``false`` otherwise.
        """
        return CONSTANT_TYPE_PORTABLE == self.get_type()

    def has_partition_hash(self):
        """
        Determines whether this Data has partition hash or not.

        :return: (bool), ``true`` if Data has partition hash, ``false`` otherwise.
        """
        return self._buffer is not None \
               and len(self._buffer) >= HEAP_DATA_OVERHEAD \
               and unpack_from(FMT_BE_INT, self._buffer, PARTITION_HASH_OFFSET)[0] != 0

    def hash_code(self):
        """
        Returns the murmur hash of the internal data.

        :return: the murmur hash of the internal data.
        """
        return murmur_hash3_x86_32(self._buffer, DATA_OFFSET, self.data_size())

    def __hash__(self):
        return self.hash_code()

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.total_size() == other.total_size() and self._buffer == other.to_bytes()

    def __len__(self):
        return self.total_size()

