import time

from tests.base import HazelcastTestCase, SingleMemberTestCase
from hazelcast.config import ClientConfig, ClientProperties
from hazelcast.client import HazelcastClient
from hazelcast.lifecycle import LifecycleState
from tests.hzrc.ttypes import Lang
from tests.util import configure_logging


class ClientTest(HazelcastTestCase):
    @classmethod
    def setUpClass(cls):
        configure_logging()

    def test_client_only_listens(self):
        rc = self.create_rc()
        client_heartbeat_seconds = 8

        cluster_config = """<hazelcast xmlns="http://www.hazelcast.com/schema/config"
           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
           xsi:schemaLocation="http://www.hazelcast.com/schema/config
           http://www.hazelcast.com/schema/config/hazelcast-config-4.0.xsd">
            <properties>
                <property name="hazelcast.client.max.no.heartbeat.seconds">{}</property>
            </properties>
        </hazelcast>""".format(client_heartbeat_seconds)
        cluster = self.create_cluster(rc, cluster_config)
        cluster.start_member()

        config = ClientConfig()
        config.cluster_name = cluster.id
        config.set_property(ClientProperties.HEARTBEAT_INTERVAL.name, 1000)

        client1 = HazelcastClient(config)

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

        config2 = ClientConfig()
        config2.cluster_name = cluster.id
        client2 = HazelcastClient(config2)

        key = "topic-name"
        topic = client1.get_topic(key)

        def message_listener(e):
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
        config = ClientConfig()
        config.cluster_name = self.cluster.id

        self.create_client(config)
        self.assertIsNone(self.get_labels_from_member())

    def test_provided_labels_are_received(self):
        config = ClientConfig()
        config.cluster_name = self.cluster.id
        config.labels.add("test-label")
        self.create_client(config)
        self.assertEqual(b"test-label", self.get_labels_from_member())

    def get_labels_from_member(self):
        script = "var client = instance_0.getClientService().getConnectedClients().iterator().next();\n" \
                 "result = client.getLabels().iterator().next();\n"
        return self.rc.executeOnController(self.cluster.id, script, Lang.JAVASCRIPT).result

