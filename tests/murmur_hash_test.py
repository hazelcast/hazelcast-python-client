import unittest

from hazelcast.hash import murmur_hash3_x86_32, hash_to_index


class HashTest(unittest.TestCase):
    def test_hash(self):
        expected = [
            ("key-1", 1228513025, 107),
            ("key-2", 1503416236, 105),
            ("key-3", 1876349747, 218),
            ("key-4", -914632498, 181),
            ("key-5", -803210507, 111),
            ("key-6", -847942313, 115),
            ("key-7", 1196747334, 223),
            ("key-8", -1444149994, 208),
            ("key-9", 1182720020, 140),
        ]

        for key, hash, partition_id in expected:
            h = murmur_hash3_x86_32(key, 0, len(key))
            p = hash_to_index(h, 271)
            self.assertEqual(h, hash)
            self.assertEqual(p, partition_id)
