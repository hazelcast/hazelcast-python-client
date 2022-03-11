import os
import sys
import unittest

import pytest

from hazelcast import HazelcastClient
from hazelcast.config import SSLProtocol
from hazelcast.errors import IllegalStateError
from tests.base import HazelcastTestCase
from tests.util import compare_client_version, get_abs_path

current_directory = os.path.dirname(__file__)

MEMBER_CONFIG = """
<hazelcast xmlns="http://www.hazelcast.com/schema/config"
           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
           xsi:schemaLocation="http://www.hazelcast.com/schema/config
           http://www.hazelcast.com/schema/config/hazelcast-config-5.0.xsd">
    <network>
        <ssl enabled="true">
            <factory-class-name>
                com.hazelcast.nio.ssl.BasicSSLContextFactory
            </factory-class-name>
            <properties>
                <property name="keyStore">%s</property>
                <property name="keyStorePassword">123456</property>
                <property name="keyStoreType">PKCS12</property>
                <property name="protocol">TLSv1.2</property>
            </properties>
        </ssl>
    </network>
</hazelcast>
"""


@unittest.skipIf(
    sys.version_info < (3, 7),
    "Hostname verification feature requires Python 3.7+",
)
@unittest.skipIf(
    compare_client_version("5.1") < 0,
    "Tests the features added in 5.1 version of the client",
)
@pytest.mark.enterprise
class SslHostnameVerificationTest(HazelcastTestCase):
    def setUp(self):
        self.rc = self.create_rc()
        self.cluster = None

    def tearDown(self):
        self.shutdown_all_clients()
        self.rc.terminateCluster(self.cluster.id)
        self.rc.exit()

    def test_hostname_verification_with_loopback_san(self):
        # SAN entry is present with different possible values
        self.start_member_with("tls-host-loopback-san.p12")

        self.start_client_with("tls-host-loopback-san.pem", "127.0.0.1:5701")
        self.start_client_with("tls-host-loopback-san.pem", "localhost:5701")

    def test_hostname_verification_with_loopback_dns_san(self):
        # SAN entry is present, but only with `dns:localhost`
        self.start_member_with("tls-host-loopback-san-dns.p12")
        self.start_client_with("tls-host-loopback-san-dns.pem", "localhost:5701")

        with self.assertRaisesRegex(IllegalStateError, "Unable to connect to any cluster"):
            self.start_client_with("tls-host-loopback-san-dns.pem", "127.0.0.1:5701")

    def test_hostname_verification_with_different_san(self):
        # There is a valid entry, but it does not match with the address of the member.
        self.start_member_with("tls-host-not-our-san.p12")

        with self.assertRaisesRegex(IllegalStateError, "Unable to connect to any cluster"):
            self.start_client_with("tls-host-not-our-san.pem", "localhost:5701")

        with self.assertRaisesRegex(IllegalStateError, "Unable to connect to any cluster"):
            self.start_client_with("tls-host-loopback-san-dns.pem", "127.0.0.1:5701")

    def test_hostname_verification_with_loopback_cn(self):
        # No entry in SAN but an entry in CN which checked as a fallback
        # when no entry in SAN is present.
        self.start_member_with("tls-host-loopback-cn.p12")

        self.start_client_with("tls-host-loopback-cn.pem", "localhost:5701")

        # See https://stackoverflow.com/a/8444863/12394291. IP addresses in CN
        # are not supported. So, we don't have a test for it.
        with self.assertRaisesRegex(IllegalStateError, "Unable to connect to any cluster"):
            self.start_client_with("tls-host-loopback-cn.pem", "127.0.0.1:5701")

    def test_hostname_verification_with_no_entry(self):
        # No entry either in the SAN or CN. No way to verify hostname.
        self.start_member_with("tls-host-no-entry.p12")

        with self.assertRaisesRegex(IllegalStateError, "Unable to connect to any cluster"):
            self.start_client_with("tls-host-no-entry.pem", "localhost:5701")

        with self.assertRaisesRegex(IllegalStateError, "Unable to connect to any cluster"):
            self.start_client_with("tls-host-no-entry.pem", "127.0.0.1:5701")

    def test_hostname_verification_disabled(self):
        # When hostname verification is disabled, the scenarious that
        # would fail in `test_hostname_verification_with_no_entry` will
        # no longer fail, showing that it is working as expected.
        self.start_member_with("tls-host-no-entry.p12")

        self.start_client_with("tls-host-no-entry.pem", "localhost:5701", check_hostname=False)
        self.start_client_with("tls-host-no-entry.pem", "127.0.0.1:5701", check_hostname=False)

    def start_client_with(
        self,
        truststore_name: str,
        member_address: str,
        *,
        check_hostname=True,
    ) -> HazelcastClient:
        return self.create_client(
            {
                "cluster_name": self.cluster.id,
                "cluster_members": [member_address],
                "ssl_enabled": True,
                "ssl_protocol": SSLProtocol.TLSv1_2,
                "ssl_cafile": get_abs_path(current_directory, truststore_name),
                "ssl_check_hostname": check_hostname,
                "cluster_connect_timeout": 0,
            }
        )

    def start_member_with(self, keystore_name: str) -> None:
        config = MEMBER_CONFIG % get_abs_path(current_directory, keystore_name)
        self.cluster = self.create_cluster(self.rc, config)
        self.cluster.start_member()
