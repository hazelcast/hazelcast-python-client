import unittest

from hazelcast.hash import murmur_hash3_x86_32, hash_to_index


class HashTest(unittest.TestCase):
    def test_hash(self):
        expected = [  # Expected values are from the Java implementation
            #  00000000 -> HEAP_DATA_OVERHEAD
            (b"00000000key-1", 1228513025, 107),
            (b"12345678key-1", 1228513025, 107),  # Heap data overhead should not matter
            (b"00000000key-2", 1503416236, 105),
            (b"00000000key-3", 1876349747, 218),
            (b"00000000key-4", -914632498, 181),
            (b"00000000key-5", -803210507, 111),
            (b"00000000key-6", -847942313, 115),
            (b"00000000key-7", 1196747334, 223),
            (b"00000000key-8", -1444149994, 208),
            (b"00000000key-9", 1182720020, 140),
            # Test with different lengths
            (b"00000000", -1585187909, 238),
            (b"00000000a", -1686100800, 46),
            (b"00000000ab", 312914265, 50),
            (b"00000000abc", -2068121803, 208),
            (b"00000000abcd", -973615161, 236),
            (b"", -1585187909, 238),
        ]

        for key, mm_hash, partition_id in expected:
            h = murmur_hash3_x86_32(bytearray(key))
            p = hash_to_index(h, 271)
            self.assertEqual(h, mm_hash)
            self.assertEqual(p, partition_id)
