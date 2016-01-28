from hazelcast.exception import HazelcastSerializationError
from hazelcast.serialization.api import IdentifiedDataSerializable
from tests.base import SingleMemberTestCase
from tests.util import random_string

FACTORY_ID = 1


class Task(IdentifiedDataSerializable):
    CLASS_ID = 1

    def write_data(self, object_data_output):
        pass

    def get_factory_id(self):
        return FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID


class ExecutorTest(SingleMemberTestCase):
    def setUp(self):
        self.executor = self.client.get_executor(random_string()).blocking()

    def test_execute_on_key_owner(self):
        # TODO: Task must be defined on the server
        with self.assertRaises(HazelcastSerializationError):
            self.executor.execute_on_key_owner("key", Task())

    def test_execute_on_member(self):
        # TODO: Task must be defined on the server
        with self.assertRaises(HazelcastSerializationError):
            self.executor.execute_on_member(self.client.cluster.members[0], Task())

    def test_execute_on_members(self):
        # TODO: Task must be defined on the server
        with self.assertRaises(HazelcastSerializationError):
            self.executor.execute_on_members(self.client.cluster.members, Task())

    def test_execute_on_all_members(self):
        # TODO: Task must be defined on the server
        with self.assertRaises(HazelcastSerializationError):
            self.executor.execute_on_all_members(Task())

    def test_shutdown(self):
        self.executor.shutdown()
        self.assertTrue(self.executor.is_shutdown())

    def tearDown(self):
        self.executor.shutdown()
        self.executor.destroy()

    def test_str(self):
        self.assertTrue(str(self.executor).startswith("Executor"))
