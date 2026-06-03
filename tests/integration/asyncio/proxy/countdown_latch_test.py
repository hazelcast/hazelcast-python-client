import asyncio
import os

import pytest

from hazelcast.errors import DistributedObjectDestroyedError, OperationTimeoutError
from hazelcast.util import AtomicInteger
from tests.integration.asyncio.base import CPTestCase
from tests.util import random_string, get_current_timestamp

inf = 2**31 - 1


@pytest.mark.enterprise
class CountDownLatchTest(CPTestCase):
    async def test_latch_in_another_group(self):
        latch = await self.get_latch()
        another_latch = await self.client.cp_subsystem.get_count_down_latch(
            latch._proxy_name + "@another"
        )
        await another_latch.try_set_count(42)
        self.assertEqual(42, await another_latch.get_count())
        self.assertNotEqual(42, await latch.get_count())

    async def test_use_after_destroy(self):
        latch = await self.get_latch()
        await latch.destroy()
        # the next destroy call should be ignored
        await latch.destroy()

        try:
            await latch.get_count()
        except DistributedObjectDestroyedError:
            pass
        else:
            self.fail("expected DistributedObjectDestroyedError to be raised")

        latch2 = await self.client.cp_subsystem.get_count_down_latch(latch._proxy_name)

        try:
            await latch2.get_count()
        except DistributedObjectDestroyedError:
            pass
        else:
            self.fail("expected DistributedObjectDestroyedError to be raised")

    async def test_await_latch_negative_timeout(self):
        latch = await self.get_latch(1)
        self.assertFalse(await latch.await_latch(-1))

    async def test_await_latch_zero_timeout(self):
        latch = await self.get_latch(1)
        self.assertFalse(await latch.await_latch(0))

    async def test_await_latch_with_timeout(self):
        timeout = 1
        latch = await self.get_latch(1)
        start = get_current_timestamp()
        self.assertFalse(await latch.await_latch(timeout))
        time_passed = get_current_timestamp() - start
        expected_time_passed = timeout
        if os.name == "nt":
            # On Windows, we were getting random test failures due to expected
            # time passed being slightly less than the timeout. This is due to
            # the low time resolution there (15-16ms). If we are on Windows, we
            # lower our expectations and settle for a slightly lower value.
            expected_time_passed *= 0.95

        self.assertTrue(
            time_passed >= expected_time_passed,
            "Time passed is less than %s, which is %s" % (expected_time_passed, time_passed),
        )

    async def test_await_latch_multiple_waiters(self):
        latch = await self.get_latch(1)
        # TODO: replace the following with the asyncio variant when implemented
        completed = AtomicInteger()

        async def run():
            await latch.await_latch(inf)
            completed.get_and_increment()

        count = 10
        tasks = []
        for _ in range(count):
            tasks.append(asyncio.create_task(run()))

        await latch.count_down()

        def assertion():
            self.assertEqual(count, completed.get())

        await self.assertTrueEventually(assertion)

    async def test_await_latch_response_on_count_down(self):
        latch = await self.get_latch()
        self.assertTrue(await latch.await_latch(inf))
        self.assertTrue(await latch.try_set_count(1))
        # make a non-blocking request
        future = asyncio.create_task(latch.await_latch(inf))
        asyncio.create_task(latch.count_down())
        self.assertTrue(await future)

    async def test_count_down(self):
        latch = await self.get_latch(10)

        for i in range(9, -1, -1):
            self.assertIsNone(await latch.count_down())
            self.assertEqual(i, await latch.get_count())

    async def test_count_down_retry_on_timeout(self):
        latch = await self.get_latch(1)
        original = latch._request_count_down
        # TODO: replace the following with the asyncio variant when implemented
        called_count = AtomicInteger()

        async def mock(expected_round, invocation_uuid):
            if called_count.get_and_increment() < 2:
                raise OperationTimeoutError("xx")
            return await original(expected_round, invocation_uuid)

        latch._request_count_down = mock
        await latch.count_down()
        # Will resolve on it's third call. First 2 throws timeout error
        self.assertEqual(3, called_count.get())
        self.assertEqual(0, await latch.get_count())

    async def test_get_count(self):
        latch = await self.get_latch(1)
        self.assertEqual(1, await latch.get_count())
        await latch.count_down()
        self.assertEqual(0, await latch.get_count())
        await latch.try_set_count(10)
        self.assertEqual(10, await latch.get_count())

    async def test_try_set_count(self):
        latch = await self.get_latch()
        self.assertTrue(await latch.try_set_count(3))
        self.assertEqual(3, await latch.get_count())

    async def test_try_set_count_when_count_is_already_set(self):
        latch = await self.get_latch(1)
        self.assertFalse(await latch.try_set_count(10))
        self.assertFalse(await latch.try_set_count(20))
        self.assertEqual(1, await latch.get_count())

    async def test_try_set_count_when_count_goes_to_zero(self):
        latch = await self.get_latch(1)
        await latch.count_down()
        self.assertEqual(0, await latch.get_count())
        self.assertTrue(await latch.try_set_count(3))
        self.assertEqual(3, await latch.get_count())

    async def get_latch(self, initial_count=None):
        latch = await self.client.cp_subsystem.get_count_down_latch("latch-" + random_string())
        if initial_count is not None:
            self.assertTrue(await latch.try_set_count(initial_count))
        return latch
