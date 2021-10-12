import time
import unittest

from tests.base import HazelcastTestCase, SingleMemberTestCase
from hazelcast.client import HazelcastClient
from hazelcast.lifecycle import LifecycleState
from tests.hzrc.ttypes import Lang
from tests.util import get_current_timestamp, compare_client_version, random_string


class ClientTest(HazelcastTestCase):
    def test_client_only_listens(self):
        rc = self.create_rc()
        client_heartbeat_seconds = 4

        cluster_config = (
            """
        <hazelcast xmlns="http://www.hazelcast.com/schema/config"
           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
           xsi:schemaLocation="http://www.hazelcast.com/schema/config
           http://www.hazelcast.com/schema/config/hazelcast-config-4.0.xsd">
            <properties>
                <property name="hazelcast.client.max.no.heartbeat.seconds">%s</property>
            </properties>
        </hazelcast>"""
            % client_heartbeat_seconds
        )
        cluster = self.create_cluster(rc, cluster_config)
        cluster.start_member()

        client1 = HazelcastClient(cluster_name=cluster.id, heartbeat_interval=1)

        def lifecycle_event_collector():
            events = []

            def event_collector(e):
                if e == LifecycleState.DISCONNECTED:
                    events.append(e)

            event_collector.events = events
            return event_collector

        collector = lifecycle_event_collector()
        client1.lifecycle_service.add_listener(collector)

        client2 = HazelcastClient(cluster_name=cluster.id)

        key = "topic-name"
        topic = client1.get_topic(key)

        def message_listener(_):
            pass

        topic.add_listener(message_listener)

        topic2 = client2.get_topic(key)
        begin = get_current_timestamp()

        while (get_current_timestamp() - begin) < 2 * client_heartbeat_seconds:
            topic2.publish("message")
            time.sleep(0.5)

        self.assertEqual(0, len(collector.events))
        client1.shutdown()
        client2.shutdown()
        rc.exit()


class ClientLabelsTest(HazelcastTestCase):
    @classmethod
    def setUpClass(cls):
        cls.rc = cls.create_rc()
        cls.cluster = cls.create_cluster(cls.rc)
        cls.cluster.start_member()

    @classmethod
    def tearDownClass(cls):
        cls.rc.terminateCluster(cls.cluster.id)
        cls.rc.exit()

    def tearDown(self):
        self.shutdown_all_clients()

    def test_default_config(self):
        client = self.create_client({"cluster_name": self.cluster.id})
        self.assertIsNone(self.get_labels_from_member(client._connection_manager.client_uuid))

    def test_provided_labels_are_received(self):
        client = self.create_client(
            {
                "cluster_name": self.cluster.id,
                "labels": [
                    "test-label",
                ],
            }
        )
        self.assertEqual(
            b"test-label", self.get_labels_from_member(client._connection_manager.client_uuid)
        )

    def get_labels_from_member(self, client_uuid):
        script = """
        var clients = instance_0.getClientService().getConnectedClients().toArray();
        for (i=0; i < clients.length; i++) {
            var client = clients[i];
            if ("%s".equals(client.getUuid().toString())) {
                result = client.getLabels().iterator().next();
                break;
            }
        }""" % str(
            client_uuid
        )
        return self.rc.executeOnController(self.cluster.id, script, Lang.JAVASCRIPT).result


@unittest.skipIf(
    compare_client_version("5.1") < 0, "Tests the features added in 4.1 version of the client"
)
class ClientTcpMetricsTest(SingleMemberTestCase):
    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        return config

    def test_bytes_received(self):
        reactor = self.client._reactor

        bytes_received = reactor.bytes_received
        self.assertGreater(bytes_received, 0)

        m = self.client.get_map(random_string()).blocking()
        m.get(random_string())

        self.assertGreater(reactor.bytes_received, bytes_received)

    def test_bytes_sent(self):
        reactor = self.client._reactor

        bytes_sent = reactor.bytes_sent
        self.assertGreater(bytes_sent, 0)

        m = self.client.get_map(random_string()).blocking()
        m.set(random_string(), random_string())

        self.assertGreater(reactor.bytes_sent, bytes_sent)
