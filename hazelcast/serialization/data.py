from _struct import *

from hazelcast.serialization import *

PARTITION_HASH_OFFSET = 0
TYPE_OFFSET = 4
DATA_OFFSET = 8

HEAP_DATA_OVERHEAD = DATA_OFFSET


class Data(object):
    """
    Data is basic unit of serialization. It stores binary form of an object serialized by serializaton service
    """

    def __init__(self, buff=None):
        self._buffer = buff

    def to_bytes(self):
        """
        :return:  Returns byte array representation of internal binary format.
        """
        return self._buffer

    def get_type(self):
        """
        :return:Returns serialization type of binary form.
        """
        if self.total_size() == 0:
            return CONSTANT_TYPE_NULL
        return unpack_from(FMT_BE_INT, self._buffer, TYPE_OFFSET)[0]

    def total_size(self):
        """
        :return:Returns the total size of Data in bytes
        """
        return len(self._buffer) if self._buffer is not None else 0

    def data_size(self):
        """
        :return:Returns size of internal binary data in bytes
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
        Returns true if this Data is created from a {@link com.hazelcast.nio.serialization.Portable} object,false otherwise.
        :return: true if source object is Portable, false otherwise.
        """
        return CONSTANT_TYPE_PORTABLE == self.get_type()

    def has_partition_hash(self):
        """
        :return: true if Data has partition hash, false otherwise.
        """
        return self._buffer is not None \
               and len(self._buffer) >= HEAP_DATA_OVERHEAD \
               and unpack_from(FMT_BE_INT, self._buffer, PARTITION_HASH_OFFSET)[0] != 0

    def hash_code(self):
        """
        :return: return the murmur mash of the internal data
        """
        return murmur_hash3_x86_32(self._buffer, DATA_OFFSET, self.data_size())

    def __hash__(self):
        return self.hash_code()

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.total_size() == other.total_size() and self._buffer == other.to_bytes()

    def __len__(self):
        return self.total_size()


def murmur_hash3_x86_32(data, offset, size, seed=0x01000193):
    """
    murmur3 hash function to determine partition
    :param data: input byte array
    :param offset: offset
    :param size: byte length
    :param seed: murmur hash seed hazelcast uses 0x01000193
    :return: calculated hash value. it will be a 32 bit int
    """
    key = bytearray(data[offset: offset + size])

    def fmix(h):
        h ^= h >> 16
        h = (h * 0x85ebca6b) & 0xFFFFFFFF
        h ^= h >> 13
        h = (h * 0xc2b2ae35) & 0xFFFFFFFF
        h ^= h >> 16
        return h;

    length = len(key)
    nblocks = int(length / 4)

    h1 = seed;

    c1 = 0xcc9e2d51
    c2 = 0x1b873593

    # body
    for block_start in xrange(0, nblocks * 4, 4):
        # ??? big endian?
        k1 = key[block_start + 3] << 24 | \
             key[block_start + 2] << 16 | \
             key[block_start + 1] << 8 | \
             key[block_start + 0]

        k1 = c1 * k1 & 0xFFFFFFFF
        k1 = (k1 << 15 | k1 >> 17) & 0xFFFFFFFF  # inlined ROTL32
        k1 = (c2 * k1) & 0xFFFFFFFF;

        h1 ^= k1
        h1 = (h1 << 13 | h1 >> 19) & 0xFFFFFFFF  # inlined _ROTL32
        h1 = (h1 * 5 + 0xe6546b64) & 0xFFFFFFFF

    # tail
    tail_index = nblocks * 4
    k1 = 0
    tail_size = length & 3

    if tail_size >= 3:
        k1 ^= key[tail_index + 2] << 16
    if tail_size >= 2:
        k1 ^= key[tail_index + 1] << 8
    if tail_size >= 1:
        k1 ^= key[tail_index + 0]

    if tail_size != 0:
        k1 = (k1 * c1) & 0xFFFFFFFF
        k1 = (k1 << 15 | k1 >> 17) & 0xFFFFFFFF  # _ROTL32
        k1 = (k1 * c2) & 0xFFFFFFFF
        h1 ^= k1

    return fmix(h1 ^ length)
