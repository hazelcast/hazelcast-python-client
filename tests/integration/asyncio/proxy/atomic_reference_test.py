import pytest

from hazelcast.errors import DistributedObjectDestroyedError, ClassCastError
from tests.integration.asyncio.base import CPTestCase
from tests.integration.backward_compatible.proxy.cp.atomic_reference_test import AppendString
from tests.util import skip_if_server_version_older_than


@pytest.mark.enterprise
class AtomicReferenceTest(CPTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.ref = await self.client.cp_subsystem.get_atomic_reference("ref")

    async def asyncTearDown(self):
        await self.ref.clear()
        await super().asyncTearDown()

    async def test_ref_in_another_group(self):
        another_ref = await self.client.cp_subsystem.get_atomic_reference("ref@mygroup")
        await another_ref.set("hey")
        self.assertEqual("hey", await another_ref.get())
        # the following value has to be None,
        # as `ref` belongs to the default CP group
        self.assertIsNone(await self.ref.get())

    async def test_use_after_destroy(self):
        another_ref = await self.client.cp_subsystem.get_atomic_reference("another-ref")
        await another_ref.destroy()
        # the next destroy call should be ignored
        await another_ref.destroy()

        try:
            await another_ref.get()
        except DistributedObjectDestroyedError:
            pass
        else:
            self.fail("expected DistributedObjectDestroyedError to be raised")

        another_ref2 = await self.client.cp_subsystem.get_atomic_reference("another-ref")

        try:
            await another_ref2.get()
        except DistributedObjectDestroyedError:
            pass
        else:
            self.fail("expected DistributedObjectDestroyedError to be raised")

    async def test_initial_value(self):
        self.assertIsNone(await self.ref.get())

    async def test_compare_and_set_when_condition_is_met(self):
        self.assertTrue(await self.ref.compare_and_set(None, 42))
        self.assertEqual(42, await self.ref.get())
        self.assertTrue(await self.ref.compare_and_set(42, "hey"))
        self.assertEqual("hey", await self.ref.get())

    async def test_compare_and_set_when_condition_is_not_met(self):
        self.assertFalse(await self.ref.compare_and_set(42, 23))
        self.assertIsNone(await self.ref.get())
        await self.ref.set("a")
        self.assertFalse(await self.ref.compare_and_set("b", "c"))
        self.assertEqual("a", await self.ref.get())

    async def test_get(self):
        await self.ref.set([1, 2, 3])
        self.assertEqual([1, 2, 3], await self.ref.get())

    async def test_set(self):
        self.assertIsNone(await self.ref.set("abc"))
        self.assertEqual("abc", await self.ref.get())
        self.assertIsNone(await self.ref.set(["another_type", 1]))
        self.assertEqual(["another_type", 1], await self.ref.get())

    async def test_get_and_set(self):
        self.assertIsNone(await self.ref.get_and_set("42"))
        self.assertEqual("42", await self.ref.get())
        self.assertEqual("42", await self.ref.get_and_set(42))
        self.assertEqual(42, await self.ref.get())

    async def test_is_none(self):
        self.assertTrue(await self.ref.is_none())
        await self.ref.set(11)
        self.assertFalse(await self.ref.is_none())
        await self.ref.set(None)
        self.assertTrue(await self.ref.is_none())

    async def test_clear(self):
        self.assertIsNone(await self.ref.clear())
        self.assertIsNone(await self.ref.get())
        await self.ref.set("str")
        self.assertEqual("str", await self.ref.get())
        self.assertIsNone(await self.ref.clear())
        self.assertIsNone(await self.ref.get())

    async def test_contains(self):
        self.assertTrue(await self.ref.contains(None))
        await self.ref.set("42")
        self.assertTrue(await self.ref.contains("42"))
        self.assertFalse(await self.ref.contains(42))
        self.assertFalse(await self.ref.contains(None))
        await self.ref.clear()
        self.assertFalse(await self.ref.contains("42"))
        self.assertTrue(await self.ref.contains(None))

    async def test_alter(self):
        # the class is defined in the 4.1 JAR
        skip_if_server_version_older_than(self, self.client, "4.1")
        await self.ref.set("hey")
        self.assertIsNone(await self.ref.alter(AppendString("123")))
        self.assertEqual("hey123", await self.ref.get())

    async def test_alter_with_incompatible_types(self):
        # the class is defined in the 4.1 JAR
        skip_if_server_version_older_than(self, self.client, "4.1")
        await self.ref.set(42)

        try:
            await self.ref.alter(AppendString("."))
        except ClassCastError:
            pass
        else:
            self.fail("expected ClassCastError to be raised")

    async def test_alter_and_get(self):
        # the class is defined in the 4.1 JAR
        skip_if_server_version_older_than(self, self.client, "4.1")
        await self.ref.set("123")
        self.assertEqual("123...", await self.ref.alter_and_get(AppendString("...")))
        self.assertEqual("123...", await self.ref.get())

    async def test_get_and_alter(self):
        # the class is defined in the 4.1 JAR
        skip_if_server_version_older_than(self, self.client, "4.1")
        await self.ref.set("hell")
        self.assertEqual("hell", await self.ref.get_and_alter(AppendString("o")))
        self.assertEqual("hello", await self.ref.get())

    async def test_apply(self):
        # the class is defined in the 4.1 JAR
        skip_if_server_version_older_than(self, self.client, "4.1")
        await self.ref.set("hell")
        self.assertEqual("hello", await self.ref.apply(AppendString("o")))
        self.assertEqual("hell", await self.ref.get())
