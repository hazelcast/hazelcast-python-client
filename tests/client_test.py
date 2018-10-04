import time

from tests.base import HazelcastTestCase
from hazelcast.config import ClientConfig, ClientProperties
from hazelcast.client import HazelcastClient
from hazelcast.lifecycle import LIFECYCLE_STATE_DISCONNECTED


class ClientTest(HazelcastTestCase):
    def test_client_only_listens(self):
        rc = self.create_rc()
        client_heartbeat_seconds = 8

        cluster_config = """<hazelcast xsi:schemaLocation="http://www.hazelcast.com/schema/config hazelcast-config-3.10.xsd"
           xmlns="http://www.hazelcast.com/schema/config"
           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
            <properties>
                <property name="hazelcast.client.max.no.heartbeat.seconds">{}</property>
            </properties>
        </hazelcast>""".format(client_heartbeat_seconds)
        cluster = self.create_cluster(rc, cluster_config)
        member = cluster.start_member()

        client_config = ClientConfig()
        client_config.set_property(ClientProperties.HEARTBEAT_INTERVAL.name, 1000)

        client1 = HazelcastClient(client_config)

        def lifecycle_event_collector():
            events = []

            def event_collector(e):
                if e == LIFECYCLE_STATE_DISCONNECTED:
                    events.append(e)

            event_collector.events = events
            return event_collector

        collector = lifecycle_event_collector()
        client1.lifecycle.add_listener(collector)
        client2 = HazelcastClient()

        key = "topic-name"
        topic = client1.get_topic(key)

        def message_listener(e):
            pass
        
        topic.add_listener(message_listener)

        client2topic = client2.get_topic(key)
        begin = time.time()

        while (time.time() - begin) < 2 * client_heartbeat_seconds:
            client2topic.publish("message")
            time.sleep(0.5)

        self.assertEqual(0, len(collector.events))
        client1.shutdown()
        client2.shutdown()
        rc.exit()
