from hazelcast.proxy.id_generator import BLOCK_SIZE
from tests.base import SingleMemberTestCase
from tests.util import random_string


class IdGeneratorTest(SingleMemberTestCase):
    def setUp(self):
        self.id_gen = self.client.get_id_generator(random_string()).blocking()

    def test_create_proxy(self):
        self.assertTrue(self.id_gen)

    def test_init(self):
        init = self.id_gen.init(10)
        self.assertTrue(init)

    def test_new_id(self):
        self.id_gen.init(10)
        new_id = self.id_gen.new_id()
        self.assertEqual(new_id, 11)

    def test_str(self):
        self.assertTrue(str(self.id_gen).startswith("IdGenerator"))

    def test_new_block(self):
        self.id_gen.init(BLOCK_SIZE - 1)
        self.assertEqual(self.id_gen.new_id(), BLOCK_SIZE)
        self.assertEqual(self.id_gen.new_id(), BLOCK_SIZE + 1)