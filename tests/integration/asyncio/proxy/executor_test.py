import os

from hazelcast.serialization.api import IdentifiedDataSerializable
from tests.integration.asyncio.base import SingleMemberTestCase
from tests.integration.backward_compatible.util import (
    read_string_from_input,
    write_string_to_output,
)
from tests.util import random_string


class AppendTask(IdentifiedDataSerializable):
    """Client side version of com.hazelcast.client.test.executor.tasks.AppendCallable"""

    def __init__(self, message):
        self.message = message

    def write_data(self, object_data_output):
        write_string_to_output(object_data_output, self.message)

    def read_data(self, object_data_input):
        self.message = read_string_from_input(object_data_input)

    def get_factory_id(self):
        return 66

    def get_class_id(self):
        return 5


APPENDAGE = ":CallableResult"  # defined on the server side


class ExecutorTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        return config

    @classmethod
    def configure_cluster(cls):
        path = os.path.abspath(__file__)
        dir_path = os.path.dirname(path)
        with open(os.path.join(dir_path, "../../backward_compatible/proxy/hazelcast.xml")) as f:
            return f.read()

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.executor = await self.client.get_executor(random_string())
        self.message = random_string()
        self.task = AppendTask(self.message)

    async def asyncTearDown(self):
        await self.executor.shutdown()
        await self.executor.destroy()
        await super().asyncTearDown()

    async def test_execute_on_key_owner(self):
        result = await self.executor.execute_on_key_owner("key", self.task)
        self.assertEqual(self.message + APPENDAGE, result)

    async def test_execute_on_member(self):
        member = self.client.cluster_service.get_members()[0]
        result = await self.executor.execute_on_member(member, self.task)
        self.assertEqual(self.message + APPENDAGE, result)

    async def test_execute_on_members(self):
        members = self.client.cluster_service.get_members()
        result = await self.executor.execute_on_members(members, self.task)
        self.assertEqual([self.message + APPENDAGE], result)

    async def test_execute_on_all_members(self):
        result = await self.executor.execute_on_all_members(self.task)
        self.assertEqual([self.message + APPENDAGE], result)

    async def test_shutdown(self):
        await self.executor.shutdown()
        self.assertTrue(await self.executor.is_shutdown())

    async def test_str(self):
        self.assertTrue(str(self.executor).startswith("Executor"))
