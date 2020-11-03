import unittest

from hazelcast.hash import murmur_hash3_x86_32, hash_to_index


class HashTest(unittest.TestCase):
    def test_hash(self):
        expected = [
            (b"00000000key-1", 1228513025, 107),
            (b"00000000key-2", 1503416236, 105),
            (b"00000000key-3", 1876349747, 218),
            (b"00000000key-4", -914632498, 181),
            (b"00000000key-5", -803210507, 111),
            (b"00000000key-6", -847942313, 115),
            (b"00000000key-7", 1196747334, 223),
            (b"00000000key-8", -1444149994, 208),
            (b"00000000key-9", 1182720020, 140),
        ]

        for key, mm_hash, partition_id in expected:
            h = murmur_hash3_x86_32(key)
            p = hash_to_index(h, 271)
            self.assertEqual(h, mm_hash)
            self.assertEqual(p, partition_id)
