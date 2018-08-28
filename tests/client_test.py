import time
import logging

from tests.base import HazelcastTestCase
from hazelcast.config import ClientConfig, PROPERTY_HEARTBEAT_INTERVAL
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
        client_config._properties[PROPERTY_HEARTBEAT_INTERVAL] = 1000

        client1 = HazelcastClient(client_config)
        is_client_disconnected = [False]

        def lifecycle_listener(event, flag=is_client_disconnected):
            if event == LIFECYCLE_STATE_DISCONNECTED:
                flag[0] = True

        client1.lifecycle.add_listener(lifecycle_listener)
        client2 = HazelcastClient()

        key = "topic-name"
        topic = client1.get_topic(key).blocking()

        def message_listener(e):
            pass
        
        topic.add_listener(message_listener)

        client2topic = client2.get_topic(key)
        begin = time.time()

        while (time.time() - begin) < 2 * client_heartbeat_seconds:
            client2topic.publish("message")

        self.assertFalse(is_client_disconnected[0])

        rc.exit()
