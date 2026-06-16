import asyncio

import pytest

from hazelcast.errors import DistributedObjectDestroyedError, IllegalMonitorStateError
from hazelcast.internal.asyncio_client import HazelcastClient
from hazelcast.internal.asyncio_proxy.fenced_lock import FencedLock
from tests.integration.asyncio.base import CPTestCase
from tests.util import random_string


@pytest.mark.enterprise
class FencedLockTest(CPTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.lock = await self.client.cp_subsystem.get_lock(random_string())
        self.initial_acquire_count = self.get_initial_acquire_count()

    async def tearDown(self):
        await self.lock.destroy()
        await super().asyncTearDown()

    async def test_lock_in_another_group(self):
        another_lock = await self.client.cp_subsystem.get_lock(self.lock._proxy_name + "@another")
        self.assert_valid_fence(await another_lock.lock())
        try:
            self.assertTrue(await another_lock.is_locked())
            self.assertFalse(await self.lock.is_locked())
        finally:
            await another_lock.unlock()

    async def test_lock_after_client_shutdown(self):
        another_client = await HazelcastClient.create_and_start(cluster_name=self.cluster.id)
        another_lock = await another_client.cp_subsystem.get_lock(self.lock._proxy_name)
        self.assert_valid_fence(await another_lock.lock())
        self.assertTrue(await another_lock.is_locked())
        self.assertTrue(await self.lock.is_locked())
        await another_client.shutdown()

        async def assertion():
            self.assertFalse(await self.lock.is_locked())

        await self.assertTrueEventually(assertion)

    async def test_use_after_destroy(self):
        await self.lock.destroy()
        # the next destroy call should be ignored
        await self.lock.destroy()

        try:
            await self.lock.lock()
        except DistributedObjectDestroyedError:
            pass
        else:
            self.fail("expected DistributedObjectDestroyedError to be raised")

        lock2 = await self.client.cp_subsystem.get_lock(self.lock._proxy_name)

        try:
            await lock2.lock()
        except DistributedObjectDestroyedError:
            pass
        else:
            self.fail("expected DistributedObjectDestroyedError to be raised")

    async def test_lock_when_not_locked(self):
        self.assert_valid_fence(await self.lock.lock())
        await self.assert_lock(True, True, 1)
        self.assert_session_acquire_count(1)

    async def test_lock_when_locked_by_self(self):
        fence = await self.lock.lock()
        self.assert_valid_fence(fence)
        fence2 = await self.lock.lock()
        self.assertEqual(fence, fence2)
        await self.assert_lock(True, True, 2)
        self.assert_session_acquire_count(2)
        await self.lock.unlock()
        await self.lock.unlock()
        self.assert_session_acquire_count(0)
        fence3 = await self.lock.lock()
        self.assert_valid_fence(fence3)
        self.assertGreater(fence3, fence)
        self.assert_session_acquire_count(1)

    async def test_lock_when_locked_by_another_task(self):
        async def valid_fence():
            self.assert_valid_fence(await self.lock.lock())

        task1 = asyncio.create_task(valid_fence())
        await task1
        await self.assert_lock(True, False, 1)
        task2 = asyncio.create_task(self.lock.lock())
        await asyncio.sleep(2)
        self.assertFalse(task2.done())  # lock request made in task2 should not complete
        self.assert_session_acquire_count(2)
        await self.lock.destroy()

        try:
            await task2
        except DistributedObjectDestroyedError:
            pass
        else:
            self.fail("expected DistributedObjectDestroyedError to be raised")

    async def test_try_lock_when_free(self):
        self.assert_valid_fence(await self.lock.try_lock())
        await self.assert_lock(True, True, 1)
        self.assert_session_acquire_count(1)

    async def test_try_lock_when_free_with_timeout(self):
        self.assert_valid_fence(await self.lock.try_lock(1))
        await self.assert_lock(True, True, 1)
        self.assert_session_acquire_count(1)

    async def test_try_lock_when_locked_by_self(self):
        fence = await self.lock.lock()
        self.assert_valid_fence(fence)
        fence2 = await self.lock.try_lock()
        self.assertEqual(fence, fence2)
        await self.assert_lock(True, True, 2)
        self.assert_session_acquire_count(2)
        await self.lock.unlock()
        await self.lock.unlock()
        self.assert_session_acquire_count(0)
        fence3 = await self.lock.try_lock()
        self.assert_valid_fence(fence3)
        self.assertGreater(fence3, fence)
        self.assert_session_acquire_count(1)

    async def test_try_lock_when_locked_by_self_with_timeout(self):
        fence = await self.lock.lock()
        self.assert_valid_fence(fence)
        fence2 = await self.lock.try_lock(2)
        self.assertEqual(fence, fence2)
        await self.assert_lock(True, True, 2)
        self.assert_session_acquire_count(2)
        await self.lock.unlock()
        await self.lock.unlock()
        self.assert_session_acquire_count(0)
        fence3 = await self.lock.try_lock(2)
        self.assert_valid_fence(fence3)
        self.assertGreater(fence3, fence)
        self.assert_session_acquire_count(1)

    async def test_try_lock_when_locked_by_another_task(self):
        await asyncio.create_task(self.lock.try_lock())
        self.assertFalse(await self.lock.try_lock())
        await self.assert_lock(True, False, 1)
        self.assert_session_acquire_count(1)

    async def test_try_lock_when_locked_by_another_task_with_timeout(self):
        async def valid_fence():
            self.assert_valid_fence(await self.lock.try_lock())

        await asyncio.create_task(valid_fence())
        self.assert_not_valid_fence(await self.lock.try_lock(1))
        await self.assert_lock(True, False, 1)
        self.assert_session_acquire_count(1)

    async def test_unlock_when_free(self):
        try:
            await self.lock.unlock()
        except IllegalMonitorStateError:
            pass
        else:
            self.fail("expected IllegalMonitorStateError to be raised")

        await self.assert_lock(False, False, 0)
        self.assert_session_acquire_count(0)

    async def test_unlock_when_locked_by_self(self):
        await self.lock.lock()
        self.assertIsNone(await self.lock.unlock())

        try:
            await self.lock.unlock()
        except IllegalMonitorStateError:
            pass
        else:
            self.fail("expected IllegalMonitorStateError to be raised")

        await self.assert_lock(False, False, 0)
        self.assert_session_acquire_count(0)

    async def test_unlock_multiple_times(self):
        await self.lock.lock()
        await self.lock.lock()
        await self.lock.unlock()
        await self.lock.unlock()
        await self.assert_lock(False, False, 0)
        self.assert_session_acquire_count(0)

    async def test_unlock_when_locked_by_another_task(self):
        await asyncio.create_task(self.lock.lock())

        try:
            await self.lock.unlock()
        except IllegalMonitorStateError:
            pass
        else:
            self.fail("expected IllegalMonitorStateError to be raised")

        await self.assert_lock(True, False, 1)
        self.assert_session_acquire_count(1)

    async def test_is_locked_when_free(self):
        self.assertFalse(await self.lock.is_locked())
        self.assert_session_acquire_count(0)

    async def test_is_locked_when_locked_by_self(self):
        await self.lock.lock()
        self.assertTrue(await self.lock.is_locked())
        await self.lock.unlock()
        self.assertFalse(await self.lock.is_locked())
        self.assert_session_acquire_count(0)

    async def test_is_locked_when_locked_by_another_task(self):
        await asyncio.create_task(self.lock.lock())
        self.assertTrue(await self.lock.is_locked())
        self.assert_session_acquire_count(1)

    async def test_is_locked_by_current_task_when_free(self):
        self.assertFalse(await self.lock.is_locked_by_current_task())
        self.assert_session_acquire_count(0)

    async def test_is_locked_by_current_task_when_locked_by_self(self):
        await self.lock.lock()
        self.assertTrue(await self.lock.is_locked_by_current_task())
        await self.lock.unlock()
        self.assertFalse(await self.lock.is_locked_by_current_task())
        self.assert_session_acquire_count(0)

    async def test_is_locked_by_current_task_when_locked_by_another_task(self):
        await asyncio.create_task(self.lock.lock())
        self.assertFalse(await self.lock.is_locked_by_current_task())
        self.assert_session_acquire_count(1)

    async def assert_lock(self, is_locked, is_locked_by_current_task, lock_count):
        self.assertEqual(is_locked, await self.lock.is_locked())
        self.assertEqual(is_locked_by_current_task, await self.lock.is_locked_by_current_task())
        self.assertEqual(lock_count, await self.lock.get_lock_count())

    def assert_valid_fence(self, fence):
        self.assertNotEqual(FencedLock.INVALID_FENCE, fence)

    def assert_not_valid_fence(self, fence):
        self.assertEqual(FencedLock.INVALID_FENCE, fence)

    def assert_session_acquire_count(self, expected_acquire_count):
        session = self.client._proxy_session_manager._sessions.get(self.lock.get_group_id(), None)
        if not session:
            actual = 0
        else:
            actual = session.acquire_count.get()

        self.assertEqual(self.initial_acquire_count + expected_acquire_count, actual)

    def get_initial_acquire_count(self):
        session = self.client._proxy_session_manager._sessions.get(self.lock.get_group_id(), None)
        if not session:
            return 0
        return session.acquire_count.get()
