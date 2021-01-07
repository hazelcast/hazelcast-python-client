from hazelcast.serialization import LE_UINT
from hazelcast.six.moves import range


def murmur_hash3_x86_32(data):
    """murmur3 hash function to determine partition

    Args:
        data (bytearray or bytes): Input byte array

    Returns:
        int: Calculated hash value.
    """
    length = max(len(data) - 8, 0)  # Heap data overhead
    nblocks = length // 4

    h1 = 0x01000193

    c1 = 0xCC9E2D51
    c2 = 0x1B873593

    # body
    for block_start in range(0, nblocks * 4, 4):
        # ??? big endian?
        k1 = LE_UINT.unpack_from(data, block_start + 8)[0]

        k1 = c1 * k1 & 0xFFFFFFFF
        k1 = (k1 << 15 | k1 >> 17) & 0xFFFFFFFF  # inlined ROTL32
        k1 = (c2 * k1) & 0xFFFFFFFF

        h1 ^= k1
        h1 = (h1 << 13 | h1 >> 19) & 0xFFFFFFFF  # inlined _ROTL32
        h1 = (h1 * 5 + 0xE6546B64) & 0xFFFFFFFF

    # tail
    tail_index = nblocks * 4
    k1 = 0
    tail_size = length & 3

    # Offsets below are shifted according to heap data overhead
    if tail_size >= 3:
        k1 ^= data[tail_index + 10] << 16
    if tail_size >= 2:
        k1 ^= data[tail_index + 9] << 8
    if tail_size >= 1:
        k1 ^= data[tail_index + 8]

    if tail_size != 0:
        k1 = (k1 * c1) & 0xFFFFFFFF
        k1 = (k1 << 15 | k1 >> 17) & 0xFFFFFFFF  # _ROTL32
        k1 = (k1 * c2) & 0xFFFFFFFF
        h1 ^= k1

    h1 ^= length
    h1 ^= h1 >> 16
    h1 = (h1 * 0x85EBCA6B) & 0xFFFFFFFF
    h1 ^= h1 >> 13
    h1 = (h1 * 0xC2B2AE35) & 0xFFFFFFFF
    h1 ^= h1 >> 16
    return -(h1 & 0x80000000) | (h1 & 0x7FFFFFFF)


def hash_to_index(mm_hash, length):
    if mm_hash == 0x80000000:
        return 0
    else:
        return abs(mm_hash) % length
