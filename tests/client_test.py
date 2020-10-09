import time

from tests.base import HazelcastTestCase
from hazelcast.client import HazelcastClient
from hazelcast.lifecycle import LifecycleState
from tests.hzrc.ttypes import Lang
from tests.util import configure_logging


class ClientTest(HazelcastTestCase):
    def test_client_only_listens(self):
        rc = self.create_rc()
        client_heartbeat_seconds = 8

        cluster_config = """<hazelcast xmlns="http://www.hazelcast.com/schema/config"
           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
           xsi:schemaLocation="http://www.hazelcast.com/schema/config
           http://www.hazelcast.com/schema/config/hazelcast-config-4.0.xsd">
            <properties>
                <property name="hazelcast.client.max.no.heartbeat.seconds">%s</property>
            </properties>
        </hazelcast>""" % client_heartbeat_seconds
        cluster = self.create_cluster(rc, cluster_config)
        cluster.start_member()

        client1 = HazelcastClient(cluster_name=cluster.id, heartbeat_interval=1)

        def lifecycle_event_collector():
            events = []

            def event_collector(e):
                print(e)
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
        begin = time.time()

        while (time.time() - begin) < 2 * client_heartbeat_seconds:
            topic2.publish("message")
            time.sleep(0.5)

        self.assertEqual(0, len(collector.events))
        client1.shutdown()
        client2.shutdown()
        rc.exit()


class ClientLabelsTest(HazelcastTestCase):
    @classmethod
    def setUpClass(cls):
        configure_logging()
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
        self.create_client({
            "cluster_name": self.cluster.id
        })
        self.assertIsNone(self.get_labels_from_member())

    def test_provided_labels_are_received(self):
        self.create_client({
            "cluster_name": self.cluster.id,
            "labels": [
                "test-label",
            ]
        })
        self.assertEqual(b"test-label", self.get_labels_from_member())

    def get_labels_from_member(self):
        script = "var client = instance_0.getClientService().getConnectedClients().iterator().next();\n" \
                 "result = client.getLabels().iterator().next();\n"
        return self.rc.executeOnController(self.cluster.id, script, Lang.JAVASCRIPT).result

