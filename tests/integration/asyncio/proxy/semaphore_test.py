import asyncio

import pytest

from hazelcast.errors import DistributedObjectDestroyedError, IllegalStateError
from hazelcast.internal.asyncio_client import HazelcastClient
from tests.integration.asyncio.base import CPTestCase
from tests.util import random_string, get_current_timestamp

SEMAPHORE_TYPES = [
    "sessionless",
    "sessionaware",
]


@pytest.mark.enterprise
class SemaphoreTest(CPTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.semaphore = None

    async def asyncTearDown(self):
        if self.semaphore:
            self.semaphore.destroy()
        await super().asyncTearDown()

    async def test_semaphore_in_another_group(self):
        for semaphore_type in SEMAPHORE_TYPES:
            with self.subTest(semaphore_type, semaphore_type=semaphore_type):
                semaphore = await self.get_semaphore(semaphore_type, 1)
                another_semaphore = await self.client.cp_subsystem.get_semaphore(
                    semaphore._proxy_name + "@another"
                )
                self.assertEqual(1, await semaphore.available_permits())
                self.assertEqual(0, await another_semaphore.available_permits())
                await semaphore.acquire()
                self.assertEqual(0, await semaphore.available_permits())
                self.assertEqual(0, await another_semaphore.available_permits())

    async def test_use_after_destroy(self):
        for semaphore_type in SEMAPHORE_TYPES:
            with self.subTest(semaphore_type, semaphore_type=semaphore_type):
                semaphore = await self.get_semaphore(semaphore_type)
                await semaphore.destroy()
                # the next destroy call should be ignored
                await semaphore.destroy()

                try:
                    await semaphore.init(1)
                except DistributedObjectDestroyedError:
                    pass
                else:
                    self.fail("expected DistributedObjectDestroyedError to be raised")

                semaphore2 = await self.client.cp_subsystem.get_semaphore(semaphore._proxy_name)

                try:
                    await semaphore2.init(1)
                except DistributedObjectDestroyedError:
                    pass
                else:
                    self.fail("expected DistributedObjectDestroyedError to be raised")

    async def test_session_aware_semaphore_after_client_shutdown(self):
        semaphore = await self.get_semaphore("sessionaware", 1)
        another_client = await HazelcastClient.create_and_start(cluster_name=self.cluster.id)
        another_semaphore = await another_client.cp_subsystem.get_semaphore(semaphore._proxy_name)
        await another_semaphore.acquire(1)
        self.assertEqual(0, await another_semaphore.available_permits())
        self.assertEqual(0, await semaphore.available_permits())
        await another_client.shutdown()

        async def assertion():
            self.assertEqual(1, await semaphore.available_permits())

        await self.assertTrueEventually(assertion)

    async def test_init(self):
        for semaphore_type in SEMAPHORE_TYPES:
            with self.subTest(semaphore_type, semaphore_type=semaphore_type):
                semaphore = await self.get_semaphore(semaphore_type)
                self.assertEqual(0, await semaphore.available_permits())
                self.assertTrue(await semaphore.init(10))
                self.assertEqual(10, await semaphore.available_permits())

    async def test_init_when_already_initialized(self):
        for semaphore_type in SEMAPHORE_TYPES:
            with self.subTest(semaphore_type, semaphore_type=semaphore_type):
                semaphore = await self.get_semaphore(semaphore_type)
                self.assertTrue(await semaphore.init(5))
                self.assertFalse(await semaphore.init(7))
                self.assertEqual(5, await semaphore.available_permits())

    async def test_acquire(self):
        for semaphore_type in SEMAPHORE_TYPES:
            with self.subTest(semaphore_type, semaphore_type=semaphore_type):
                semaphore = await self.get_semaphore(semaphore_type, 42)
                self.assertIsNone(await semaphore.acquire(2))
                self.assertEqual(40, await semaphore.available_permits())
                self.assertIsNone(await semaphore.acquire())
                self.assertEqual(39, await semaphore.available_permits())

    async def test_acquire_when_not_enough_permits(self):
        for semaphore_type in SEMAPHORE_TYPES:
            with self.subTest(semaphore_type, semaphore_type=semaphore_type):
                semaphore = await self.get_semaphore(semaphore_type, 5)
                f = asyncio.create_task(semaphore.acquire(10))
                self.assertFalse(f.done())
                await asyncio.sleep(2)
                self.assertFalse(f.done())
                await semaphore.destroy()

                try:
                    await f
                except DistributedObjectDestroyedError:
                    pass
                else:
                    self.fail("expected DistributedObjectDestroyedError to be raised")

    async def test_acquire_blocks_until_someone_releases(self):
        for semaphore_type in SEMAPHORE_TYPES:
            with self.subTest(semaphore_type, semaphore_type=semaphore_type):
                semaphore = await self.get_semaphore(semaphore_type, 1)
                event = asyncio.Event()
                event2 = asyncio.Event()

                async def run():
                    await semaphore.acquire(1)
                    event.set()
                    await event2.wait()
                    await asyncio.sleep(1)
                    await semaphore.release()

                t = asyncio.create_task(run())
                await event.wait()
                start = get_current_timestamp()
                f = semaphore.acquire()
                event2.set()
                await f
                self.assertGreaterEqual(get_current_timestamp() - start, 1)
                await t

    async def test_acquire_blocks_until_semaphore_is_destroyed(self):
        for semaphore_type in SEMAPHORE_TYPES:
            with self.subTest(semaphore_type, semaphore_type=semaphore_type):
                semaphore = await self.get_semaphore(semaphore_type, 1)
                event = asyncio.Event()
                event2 = asyncio.Event()

                async def run():
                    await semaphore.acquire(1)
                    event.set()
                    await event2.wait()
                    await asyncio.sleep(1)
                    await semaphore.destroy()

                t = asyncio.create_task(run())
                await event.wait()
                start = get_current_timestamp()
                f = semaphore.acquire()
                event2.set()

                try:
                    await f
                except DistributedObjectDestroyedError:
                    pass
                else:
                    self.fail("expected DistributedObjectDestroyedError to be raised")

                self.assertGreaterEqual(get_current_timestamp() - start, 1)
                await t

    async def test_available_permits(self):
        for semaphore_type in SEMAPHORE_TYPES:
            with self.subTest(semaphore_type, semaphore_type=semaphore_type):
                semaphore = await self.get_semaphore(semaphore_type)
                self.assertEqual(0, await semaphore.available_permits())
                await semaphore.init(5)
                self.assertEqual(5, await semaphore.available_permits())
                await semaphore.acquire(3)
                self.assertEqual(2, await semaphore.available_permits())

    async def test_drain_permits(self):
        for semaphore_type in SEMAPHORE_TYPES:
            with self.subTest(semaphore_type, semaphore_type=semaphore_type):
                semaphore = await self.get_semaphore(semaphore_type, 20)
                await semaphore.acquire(5)
                self.assertEqual(15, await semaphore.drain_permits())
                self.assertEqual(0, await semaphore.available_permits())

    async def test_drain_permits_when_no_permits(self):
        for semaphore_type in SEMAPHORE_TYPES:
            with self.subTest(semaphore_type, semaphore_type=semaphore_type):
                semaphore = await self.get_semaphore(semaphore_type, 0)
                self.assertEqual(0, await semaphore.drain_permits())

    async def test_reduce_permits(self):
        for semaphore_type in SEMAPHORE_TYPES:
            with self.subTest(semaphore_type, semaphore_type=semaphore_type):
                semaphore = await self.get_semaphore(semaphore_type, 10)
                self.assertIsNone(await semaphore.reduce_permits(5))
                self.assertEqual(5, await semaphore.available_permits())
                self.assertIsNone(await semaphore.reduce_permits(0))
                self.assertEqual(5, await semaphore.available_permits())

    async def test_reduce_permits_on_negative_permits_counter_sessionless(self):
        semaphore = await self.get_semaphore("sessionless", 10)
        await semaphore.reduce_permits(15)
        self.assertEqual(-5, await semaphore.available_permits())
        await semaphore.release(10)
        self.assertEqual(5, await semaphore.available_permits())

    async def test_reduce_permits_on_negative_permits_counter_juc_sessionless(self):
        semaphore = await self.get_semaphore("sessionless", 0)
        await semaphore.reduce_permits(100)
        await semaphore.release(10)
        self.assertEqual(-90, await semaphore.available_permits())
        self.assertEqual(-90, await semaphore.drain_permits())
        await semaphore.release(10)
        self.assertEqual(10, await semaphore.available_permits())
        self.assertEqual(10, await semaphore.drain_permits())

    async def test_reduce_permits_on_negative_permits_counter_session_aware(self):
        semaphore = await self.get_semaphore("sessionaware", 10)
        await semaphore.reduce_permits(15)
        self.assertEqual(-5, await semaphore.available_permits())

    async def test_reduce_permits_on_negative_permits_counter_juc_session_aware(self):
        semaphore = await self.get_semaphore("sessionaware", 0)
        await semaphore.reduce_permits(100)
        self.assertEqual(-100, await semaphore.available_permits())
        self.assertEqual(-100, await semaphore.drain_permits())

    async def test_increase_permits(self):
        for semaphore_type in SEMAPHORE_TYPES:
            with self.subTest(semaphore_type, semaphore_type=semaphore_type):
                semaphore = await self.get_semaphore(semaphore_type, 10)
                self.assertEqual(10, await semaphore.available_permits())
                self.assertIsNone(await semaphore.increase_permits(100))
                self.assertEqual(110, await semaphore.available_permits())
                self.assertIsNone(await semaphore.increase_permits(0))
                self.assertEqual(110, await semaphore.available_permits())

    async def test_release(self):
        for semaphore_type in SEMAPHORE_TYPES:
            with self.subTest(semaphore_type, semaphore_type=semaphore_type):
                semaphore = await self.get_semaphore(semaphore_type, 2)
                await semaphore.acquire(2)
                self.assertIsNone(await semaphore.release(2))
                self.assertEqual(2, await semaphore.available_permits())

    async def test_release_when_acquired_by_another_client_sessionless(self):
        semaphore = await self.get_semaphore("sessionless")
        another_client = await HazelcastClient.create_and_start(cluster_name=self.cluster.id)
        another_semaphore = await another_client.cp_subsystem.get_semaphore(semaphore._proxy_name)
        self.assertTrue(await another_semaphore.init(1))
        await another_semaphore.acquire()

        try:
            await semaphore.release(1)
            self.assertEqual(1, await semaphore.available_permits())
        finally:
            await another_client.shutdown()

    async def test_release_when_not_acquired_session_aware(self):
        semaphore = await self.get_semaphore("sessionaware", 3)
        await semaphore.acquire(1)

        try:
            await semaphore.release(2)
        except IllegalStateError:
            pass
        else:
            self.fail("expected IllegalStateError to be raised")

    async def test_release_when_there_is_no_session_session_aware(self):
        semaphore = await self.get_semaphore("sessionaware", 3)

        try:
            await semaphore.release()
        except IllegalStateError:
            pass
        else:
            self.fail("expected IllegalStateError to be raised")

    async def test_try_acquire(self):
        for semaphore_type in SEMAPHORE_TYPES:
            with self.subTest(semaphore_type, semaphore_type=semaphore_type):
                semaphore = await self.get_semaphore(semaphore_type, 5)
                self.assertTrue(await semaphore.try_acquire())
                self.assertEqual(4, await semaphore.available_permits())

    async def test_try_acquire_with_given_permits(self):
        for semaphore_type in SEMAPHORE_TYPES:
            with self.subTest(semaphore_type, semaphore_type=semaphore_type):
                semaphore = await self.get_semaphore(semaphore_type, 5)
                self.assertTrue(await semaphore.try_acquire(3))
                self.assertEqual(2, await semaphore.available_permits())

    async def test_try_acquire_when_not_enough_permits(self):
        for semaphore_type in SEMAPHORE_TYPES:
            with self.subTest(semaphore_type, semaphore_type=semaphore_type):
                semaphore = await self.get_semaphore(semaphore_type, 1)
                self.assertFalse(await semaphore.try_acquire(2))
                self.assertEqual(1, await semaphore.available_permits())

    async def test_try_acquire_when_not_enough_permits_with_timeout(self):
        for semaphore_type in SEMAPHORE_TYPES:
            with self.subTest(semaphore_type, semaphore_type=semaphore_type):
                semaphore = await self.get_semaphore(semaphore_type, 1)
                start = get_current_timestamp()
                self.assertFalse(await semaphore.try_acquire(2, 1))
                self.assertGreaterEqual(get_current_timestamp() - start, 1)
                self.assertEqual(1, await semaphore.available_permits())

    async def get_semaphore(self, semaphore_type, initialize_with=None):
        semaphore = await self.client.cp_subsystem.get_semaphore(semaphore_type + random_string())
        if initialize_with is not None:
            await semaphore.init(initialize_with)
        self.semaphore = semaphore
        return semaphore
