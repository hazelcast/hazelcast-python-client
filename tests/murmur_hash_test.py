import unittest

from hazelcast.hash import murmur_hash3_x86_32, hash_to_index


class HashTest(unittest.TestCase):
    def test_hash(self):
        expected = [
            (b"key-1", 1228513025, 107),
            (b"key-2", 1503416236, 105),
            (b"key-3", 1876349747, 218),
            (b"key-4", -914632498, 181),
            (b"key-5", -803210507, 111),
            (b"key-6", -847942313, 115),
            (b"key-7", 1196747334, 223),
            (b"key-8", -1444149994, 208),
            (b"key-9", 1182720020, 140),
        ]

        for key, hash, partition_id in expected:
            h = murmur_hash3_x86_32(key, 0, len(key))
            p = hash_to_index(h, 271)
            self.assertEqual(h, hash)
            self.assertEqual(p, partition_id)
