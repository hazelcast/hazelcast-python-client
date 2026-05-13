import asyncio

from tests.integration.asyncio.base import SingleMemberTestCase
from tests.util import random_string


class MapTest(SingleMemberTestCase):

    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        return config

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.map = await self.client.get_map(random_string())

    async def asyncTearDown(self):
        await self.map.destroy()
        await super().asyncTearDown()

    async def test_cancel(self):
        self.skipTest("broken")
        async def keep_setting():
            for i in range(1000):
                print(i)
                await self.map.set("foo", i)
                await asyncio.sleep(0)

        task = asyncio.create_task(keep_setting())
        await asyncio.sleep(0.01)
        task.cancel()
        value = await self.map.get("foo")
        self.assertGreater(value, 0)
