import asyncio
import logging
import unittest
from typing import Awaitable

from hazelcast.asyncio.client import HazelcastClient

from tests.base import _Cluster
from tests.hzrc.client import HzRemoteController
from tests.util import get_current_timestamp


class HazelcastTestCase(unittest.TestCase):
    clients = []

    def __init__(self, methodName):
        unittest.TestCase.__init__(self, methodName)
        self.logger = logging.getLogger(methodName)

    @staticmethod
    def create_rc():
        return HzRemoteController("127.0.0.1", 9701)

    @classmethod
    def create_cluster(cls, rc, config=None):
        return _Cluster(rc, rc.createCluster(None, config))

    @classmethod
    def create_cluster_keep_cluster_name(cls, rc, config=None):
        return _Cluster(rc, rc.createClusterKeepClusterName(None, config))

    async def create_client(self, config=None):
        client = await HazelcastClient.create_and_start(**config)
        self.clients.append(client)
        return client

    async def shutdown_all_clients(self):
        async with asyncio.TaskGroup() as tg:
            for c in self.clients:
                tg.create_task(c.shutdown())
        self.clients = []

    async def assertTrueEventually(self, assertion, timeout=30):
        timeout_time = get_current_timestamp() + timeout
        last_exception = None
        while get_current_timestamp() < timeout_time:
            try:
                maybe_awaitable = assertion()
                if isinstance(maybe_awaitable, Awaitable):
                    await maybe_awaitable
                return
            except AssertionError as e:
                last_exception = e
                await asyncio.sleep(0.1)
        if last_exception is None:
            raise Exception("Could not enter the assertion loop!")
        raise last_exception

    async def assertSetEventually(self, event: asyncio.Event, timeout=5):
        is_set = asyncio.wait_for(event.wait(), timeout=timeout)
        self.assertTrue(is_set, "Event was not set within %d seconds" % timeout)

    def assertEntryEvent(
        self,
        event,
        event_type,
        key=None,
        value=None,
        old_value=None,
        merging_value=None,
        number_of_affected_entries=1,
    ):

        self.assertEqual(event.key, key)
        self.assertEqual(event.event_type, event_type)
        self.assertEqual(event.value, value)
        self.assertEqual(event.merging_value, merging_value)
        self.assertEqual(event.old_value, old_value)
        self.assertEqual(event.number_of_affected_entries, number_of_affected_entries)

    def assertDistributedObjectEvent(self, event, name, service_name, event_type):
        self.assertEqual(name, event.name)
        self.assertEqual(service_name, event.service_name)
        self.assertEqual(event_type, event.event_type)

    def set_logging_level(self, level):
        logging.getLogger().setLevel(level)


class SingleMemberTestCase(unittest.IsolatedAsyncioTestCase, HazelcastTestCase):
    """
    Test cases where a single member - client combination is needed
    """

    rc = None
    cluster = None
    member = None
    client = None

    @classmethod
    def setUpClass(cls):
        cls.rc = cls.create_rc()
        cls.cluster = cls.create_cluster(cls.rc, cls.configure_cluster())
        cls.member = cls.cluster.start_member()

    @classmethod
    def tearDownClass(cls):
        cls.rc.terminateCluster(cls.cluster.id)
        cls.rc.exit()

    @classmethod
    def configure_client(cls, config):
        return config

    @classmethod
    def configure_cluster(cls):
        return None

    async def asyncSetUp(self):
        self.client = await HazelcastClient.create_and_start(**self.configure_client({}))

    async def asyncTearDown(self):
        await self.client.shutdown()
