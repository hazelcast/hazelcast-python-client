import asyncio
from asyncio import CancelledError

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

    async def test_operation_after_cancel(self):
        key = "k1"
        await self.map.set(key, "v1")
        task = asyncio.create_task(self.map.get(key))
        task.cancel()

        try:
            await task
        except CancelledError:
            pass
        else:
            self.fail("expected CancelledError to be raised")

        value = await self.map.get(key)
        self.assertEqual("v1", value)

    async def test_cancel(self):
        async def keep_setting():
            for i in range(1000):
                print(i)
                await self.map.set("foo", i)
                await asyncio.sleep(0)

        task = asyncio.create_task(keep_setting())
        await asyncio.sleep(0.1)
        task.cancel()
        value = await self.map.get("foo")
        self.assertGreater(value, 0)

    async def test_timeout(self):
        async def keep_setting():
            for i in range(1000):
                print(i)
                await self.map.set("foo", i)
                await asyncio.sleep(0)

        try:
            await asyncio.wait_for(keep_setting(), 0.1)
        except TimeoutError:
            pass
        value = await self.map.get("foo")
        self.assertGreater(value, 0)
