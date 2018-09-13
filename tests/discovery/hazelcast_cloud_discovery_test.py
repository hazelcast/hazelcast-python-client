import ssl
import os
import threading

from hazelcast.six.moves import BaseHTTPServer
from hazelcast import six
from unittest import TestCase

from hazelcast.core import Address
from hazelcast.exception import HazelcastCertificationError
from hazelcast.discovery import HazelcastCloudDiscovery, PROPERTY_CLOUD_URL_BASE
from hazelcast.config import ClientConfig
from hazelcast.client import HazelcastClient
from tests.util import get_abs_path

TOKEN = "123abc456"

CLOUD_URL = HazelcastCloudDiscovery._CLOUD_URL_PATH

RESPONSE = """[
 {"private-address":"10.47.0.8","public-address":"54.213.63.142:32298"},
 {"private-address":"10.47.0.9","public-address":"54.245.77.185:32298"},
 {"private-address":"10.47.0.10","public-address":"54.186.232.37:32298"}
]"""

HOST = "localhost"

EXPECTED_ADDRESS_DICT = {Address("10.47.0.8", 32298): Address("54.213.63.142", 32298),
                         Address("10.47.0.9", 32298): Address("54.245.77.185", 32298),
                         Address("10.47.0.10", 32298): Address("54.186.232.37", 32298)}


class CloudHTTPHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_GET(self):
        idx = self.path.find("=")
        if idx > 0:
            if self.path[:idx + 1] == CLOUD_URL:
                # Found a cluster with the given token
                if self.path[idx + 1:] == TOKEN:
                    self.send_response(200)
                    self.send_header("Content-type", "text/html")
                    self.end_headers()
                    self.wfile.write(six.b(RESPONSE))
                    return
                # Can not find a cluster with the given token
                else:
                    self.send_response(404)
                    self.send_header("Content-type", "text/html")
                    self.end_headers()
                    self.wfile.write(six.b('{"message":"Cluster with token: ' + self.path[idx + 1:] + ' not found."}'))
        else:
            # Wrong URL
            self.send_response(404)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(six.b("default backend - 404"))


class Server(object):
    cur_dir = os.path.dirname(__file__)

    def __init__(self):
        self.server = BaseHTTPServer.HTTPServer((HOST, 0), CloudHTTPHandler)
        self.server.socket = ssl.wrap_socket(self.server.socket, get_abs_path(self.cur_dir, "key.pem"),
                                             get_abs_path(self.cur_dir, "cert.pem"),
                                             server_side=True)
        self.port = self.server.socket.getsockname()[1]

    def start_server(self):
        self.server.serve_forever()

    def close_server(self):
        self.server.shutdown()


class TestClient(HazelcastClient):
    def _start(self):
        # Let the client to initialize the cloud address provider and translator, don't actually start it.
        pass


class HazelcastCloudDiscoveryTest(TestCase):
    cur_dir = os.path.dirname(__file__)

    @classmethod
    def setUpClass(cls):
        cls.server = Server()
        cls.server_thread = threading.Thread(target=cls.server.start_server)
        cls.server_thread.start()

    def test_found_response(self):
        discovery = HazelcastCloudDiscovery(HOST + ":" + str(self.server.port), CLOUD_URL + TOKEN, 5.0)
        discovery._ctx = ssl.create_default_context(cafile=get_abs_path(self.cur_dir, "cert.pem"))
        actual = discovery.discover_nodes()

        six.assertCountEqual(self, EXPECTED_ADDRESS_DICT, actual)

    def test_not_found_response(self):
        discovery = HazelcastCloudDiscovery(HOST + ":" + str(self.server.port), CLOUD_URL + "INVALID_TOKEN", 5.0)
        discovery._ctx = ssl.create_default_context(cafile=get_abs_path(self.cur_dir, "cert.pem"))
        with self.assertRaises(IOError):
            discovery.discover_nodes()

    def test_invalid_url(self):
        discovery = HazelcastCloudDiscovery(HOST + ":" + str(self.server.port), "/INVALID_URL", 5.0)
        discovery._ctx = ssl.create_default_context(cafile=get_abs_path(self.cur_dir, "cert.pem"))
        with self.assertRaises(IOError):
            discovery.discover_nodes()

    def test_invalid_certificates(self):
        discovery = HazelcastCloudDiscovery(HOST + ":" + str(self.server.port), CLOUD_URL + TOKEN, 5.0)
        with self.assertRaises(HazelcastCertificationError):
            discovery.discover_nodes()

    def test_client_with_cloud_discovery(self):
        config = ClientConfig()
        config.network_config.cloud_config.enabled = True
        config.network_config.cloud_config.discovery_token = TOKEN

        config.set_property(PROPERTY_CLOUD_URL_BASE, HOST + ":" + str(self.server.port))
        client = TestClient(config)
        client._address_translator._cloud_discovery._ctx = ssl.create_default_context(
            cafile=get_abs_path(self.cur_dir, "cert.pem"))
        client._address_providers[0]._cloud_discovery._ctx = ssl.create_default_context(
            cafile=get_abs_path(self.cur_dir, "cert.pem"))

        private_addresses = client._address_providers[0].load_addresses()

        six.assertCountEqual(self, list(EXPECTED_ADDRESS_DICT.keys()), private_addresses)

        for private_address in private_addresses:
            translated_address = client._address_translator.translate(private_address)
            self.assertEqual(EXPECTED_ADDRESS_DICT[private_address], translated_address)

    @classmethod
    def tearDownClass(cls):
        cls.server.close_server()
