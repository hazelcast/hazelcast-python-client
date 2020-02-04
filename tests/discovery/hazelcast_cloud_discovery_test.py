import ssl
import os
import threading

from hazelcast.six.moves import BaseHTTPServer
from hazelcast import six
from unittest import TestCase
from hazelcast.core import Address
from hazelcast.exception import HazelcastCertificationError
from hazelcast.discovery import HazelcastCloudDiscovery
from hazelcast.config import ClientConfig
from hazelcast.client import HazelcastClient
from tests.util import get_abs_path

TOKEN = "123abc456"
PRIVATE_LINK_TOKEN = "abc123def"

CLOUD_URL = HazelcastCloudDiscovery._CLOUD_URL_PATH

RESPONSE = """[
 {"private-address":"10.47.0.8","public-address":"54.213.63.142:32298"},
 {"private-address":"10.47.0.9","public-address":"54.245.77.185:32298"},
 {"private-address":"10.47.0.10","public-address":"54.186.232.37:32298"}
]"""

PRIVATE_LINK_RESPONSE = """[
  {"private-address":"100.96.5.1:5701","public-address":"10.113.44.139:31115"},
  {"private-address":"100.96.4.2:5701","public-address":"10.113.44.130:31115"}
]"""

HOST = "localhost"

ADDRESSES = {Address("10.47.0.8", 32298): Address("54.213.63.142", 32298),
             Address("10.47.0.9", 32298): Address("54.245.77.185", 32298),
             Address("10.47.0.10", 32298): Address("54.186.232.37", 32298)}

PRIVATE_LINK_ADDRESSES = {Address("100.96.5.1", 5701): Address("10.113.44.139", 31115),
                          Address("100.96.4.2", 5701): Address("10.113.44.130", 31115)}


class CloudHTTPHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_GET(self):
        idx = self.path.find("=")
        if idx > 0:
            if self.path[:idx + 1] == CLOUD_URL:
                # Found a cluster with the given token
                token = self.path[idx + 1:]
                if token == TOKEN:
                    self._set_response(200, RESPONSE)
                elif token == PRIVATE_LINK_TOKEN:
                    self._set_response(200, PRIVATE_LINK_RESPONSE)
                # Can not find a cluster with the given token
                else:
                    self._set_response(404, '{"message":"Cluster with token: ' + self.path[idx + 1:] + ' not found."}')
        else:
            # Wrong URL
            self._set_response(404, "default backend - 404")

    def _set_response(self, status, message):
        self.send_response(status)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(six.b(message))


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
        cls.ctx = ssl.create_default_context(cafile=get_abs_path(cls.cur_dir, "cert.pem"))
        cls.server = Server()
        cls.server_thread = threading.Thread(target=cls.server.start_server)
        cls.server_thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.close_server()

    def test_found_response(self):
        discovery = HazelcastCloudDiscovery(*get_params(HOST, self.server.port, CLOUD_URL, TOKEN))
        discovery._ctx = self.ctx
        addresses = discovery.discover_nodes()

        six.assertCountEqual(self, ADDRESSES, addresses)

    def test_private_link_response(self):
        discovery = HazelcastCloudDiscovery(*get_params(HOST, self.server.port, CLOUD_URL, PRIVATE_LINK_TOKEN))
        discovery._ctx = self.ctx
        addresses = discovery.discover_nodes()

        six.assertCountEqual(self, PRIVATE_LINK_ADDRESSES, addresses)

    def test_not_found_response(self):
        discovery = HazelcastCloudDiscovery(*get_params(HOST, self.server.port, CLOUD_URL, "INVALID_TOKEN"))
        discovery._ctx = self.ctx
        with self.assertRaises(IOError):
            discovery.discover_nodes()

    def test_invalid_url(self):
        discovery = HazelcastCloudDiscovery(*get_params(HOST, self.server.port, "/INVALID_URL", ""))
        discovery._ctx = self.ctx
        with self.assertRaises(IOError):
            discovery.discover_nodes()

    def test_invalid_certificates(self):
        discovery = HazelcastCloudDiscovery(*get_params(HOST, self.server.port, CLOUD_URL, TOKEN))
        with self.assertRaises(HazelcastCertificationError):
            discovery.discover_nodes()

    def test_client_with_cloud_discovery(self):
        config = ClientConfig()
        config.network_config.cloud_config.enabled = True
        config.network_config.cloud_config.discovery_token = TOKEN

        config.set_property(HazelcastCloudDiscovery.CLOUD_URL_BASE_PROPERTY.name, HOST + ":" + str(self.server.port))
        client = TestClient(config)
        client._address_translator.cloud_discovery._ctx = self.ctx
        client._address_providers[0].cloud_discovery._ctx = self.ctx

        private_addresses = client._address_providers[0].load_addresses()

        six.assertCountEqual(self, list(ADDRESSES.keys()), private_addresses)

        for private_address in private_addresses:
            translated_address = client._address_translator.translate(private_address)
            self.assertEqual(ADDRESSES[private_address], translated_address)


def get_params(host, port, url, token, timeout=5.0):
    return host + ":" + str(port), url + token, timeout
