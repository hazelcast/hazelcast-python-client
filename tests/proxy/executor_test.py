import os

from hazelcast.serialization.api import IdentifiedDataSerializable
from tests.base import SingleMemberTestCase
from tests.util import random_string


class _AppendTask(IdentifiedDataSerializable):
    """Client side version of com.hazelcast.client.test.executor.tasks.AppendCallable"""
    def __init__(self, message):
        self.message = message

    def write_data(self, object_data_output):
        object_data_output.write_utf(self.message)

    def read_data(self, object_data_input):
        self.message = object_data_input.read_utf()

    def get_factory_id(self):
        return 66

    def get_class_id(self):
        return 5


_APPENDAGE = ":CallableResult"  # defined on the server side


class ExecutorTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        return config

    @classmethod
    def configure_cluster(cls):
        path = os.path.abspath(__file__)
        dir_path = os.path.dirname(path)
        with open(os.path.join(dir_path, "hazelcast.xml")) as f:
            return f.read()

    def setUp(self):
        self.executor = self.client.get_executor(random_string()).blocking()
        self.message = random_string()
        self.task = _AppendTask(self.message)

    def tearDown(self):
        self.executor.shutdown()
        self.executor.destroy()

    def test_execute_on_key_owner(self):
        result = self.executor.execute_on_key_owner("key", self.task)
        self.assertEqual(self.message + _APPENDAGE, result)

    def test_execute_on_member(self):
        member = self.client.cluster_service.get_members()[0]
        result = self.executor.execute_on_member(member, self.task)
        self.assertEqual(self.message + _APPENDAGE, result)
        
    def test_execute_on_members(self):
        members = self.client.cluster_service.get_members()
        result = self.executor.execute_on_members(members, self.task)
        self.assertEqual([self.message + _APPENDAGE], result)

    def test_execute_on_all_members(self):
        result = self.executor.execute_on_all_members(self.task)
        self.assertEqual([self.message + _APPENDAGE], result)

    def test_shutdown(self):
        self.executor.shutdown()
        self.assertTrue(self.executor.is_shutdown())

    def test_str(self):
        self.assertTrue(str(self.executor).startswith("Executor"))
