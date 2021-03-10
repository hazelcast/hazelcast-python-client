from tests.base import HazelcastTestCase
from tests.hzrc.ttypes import Lang


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
