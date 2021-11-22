import os
import unittest

import pytest

from hazelcast.errors import HazelcastError
from tests.base import HazelcastTestCase
from tests.util import get_abs_path, compare_client_version
from hazelcast.client import HazelcastClient

try:
    from hazelcast.security import BasicTokenProvider
except ImportError:
    pass


@pytest.mark.enterprise
@unittest.skipIf(
    compare_client_version("4.2.1") < 0, "Tests the features added in 4.2.1 version of the client"
)
class AuthenticationTest(HazelcastTestCase):
    current_directory = os.path.dirname(__file__)
    rc = None
    hazelcast_token_xml = get_abs_path(current_directory, "hazelcast-token.xml")
    hazelcast_userpass_xml = get_abs_path(current_directory, "hazelcast-user-pass.xml")

    def setUp(self):
        self.rc = self.create_rc()

    def tearDown(self):
        self.rc.exit()

    def test_no_auth(self):
        cluster = self.create_cluster(self.rc, self.configure_cluster(self.hazelcast_userpass_xml))
        cluster.start_member()

        with self.assertRaises(HazelcastError):
            HazelcastClient(cluster_name=cluster.id, cluster_connect_timeout=2)

    def test_token_auth(self):
        cluster = self.create_cluster(self.rc, self.configure_cluster(self.hazelcast_token_xml))
        cluster.start_member()

        token_provider = BasicTokenProvider("Hazelcast")
        client = HazelcastClient(cluster_name=cluster.id, token_provider=token_provider)
        self.assertTrue(client.lifecycle_service.is_running())
        client.shutdown()

    def test_username_password_auth(self):
        cluster = self.create_cluster(self.rc, self.configure_cluster(self.hazelcast_userpass_xml))
        cluster.start_member()

        client = HazelcastClient(
            cluster_name=cluster.id, creds_username="member1", creds_password="s3crEt"
        )
        self.assertTrue(client.lifecycle_service.is_running())
        client.shutdown()

    @classmethod
    def configure_cluster(cls, filename):
        with open(filename, "r") as f:
            return f.read()
