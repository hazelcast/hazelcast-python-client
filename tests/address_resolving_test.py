from hazelcast.connection import ConnectionManager
from hazelcast.config import ClientConfig
from hazelcast.client import HazelcastClient
from tests.base import HazelcastTestCase

class AddressResolvingTest(HazelcastTestCase):

    def setUp(self):
        self.rc = self.create_rc()

    def tearDown(self):
        self.rc.exit()

    def test_connection_memberHostname_clientIP(self):
        self.connection_test("localhost", "127.0.0.1")

    def test_connection_memberHostname_clientHostname(self):
        self.connection_test("localhost", "localhost")

    def test_connection_memberIP_clientIP(self):
        self.connection_test("127.0.0.1", "127.0.0.1")

    def test_connection_memberIP_clientHostname(self):
        self.connection_test("127.0.0.1", "localhost")

    def connection_test(self, member_address, client_address):
        #Start Java hazelcast member with member address:
        member_config = """<hazelcast xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                            xsi:schemaLocation="http://www.hazelcast.com/schema/config hazelcast-config-3.11.xsd"
                            xmlns="http://www.hazelcast.com/schema/config">
                            <network>
                            <public-address>{}</public-address>
                            </network>
                            </hazelcast>""".format(member_address)
        cluster = self.create_cluster(self.rc, member_config)
        member = cluster.start_member()

        #Start python client with client address:
        config = ClientConfig()
        config.network_config.addresses.append(client_address)
        client = HazelcastClient(config)

        self.assertTrueEventually(lambda: len(client.connection_manager.connections) == 1)

        self.shutdown_all_clients()

