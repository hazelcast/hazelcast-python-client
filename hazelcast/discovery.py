import json
import logging

from hazelcast.exception import HazelcastCertificationError
from hazelcast.util import _parse_address
from hazelcast.core import Address
from hazelcast.config import ClientProperty
from hazelcast.six.moves import http_client

try:
    import ssl
except ImportError:
    ssl = None


class HazelcastCloudAddressProvider(object):
    """
    Provides initial addresses for client to find and connect to a node.
    """
    logger = logging.getLogger("HazelcastCloudAddressProvider")

    def __init__(self, host, url, connection_timeout):
        self.cloud_discovery = HazelcastCloudDiscovery(host, url, connection_timeout)

    def load_addresses(self):
        """
        Loads member addresses from Hazelcast.cloud endpoint.

        :return: (Sequence), The possible member addresses to connect to.
        """
        try:
            return list(self.cloud_discovery.discover_nodes().keys())
        except Exception as ex:
            self.logger.warning("Failed to load addresses from Hazelcast.cloud: {}".format(ex.args[0]))
        return []


class HazelcastCloudAddressTranslator(object):
    """
    Resolves private IP addresses of Hazelcast.cloud service.
    """
    logger = logging.getLogger("HazelcastAddressTranslator")

    def __init__(self, host, url, connection_timeout):
        self.cloud_discovery = HazelcastCloudDiscovery(host, url, connection_timeout)
        self._private_to_public = dict()

    def translate(self, address):
        """
        Translates the given address to another address specific to network or service.

        :param address: (:class:`~hazelcast.core.Address`), private address to be translated
        :return: (:class:`~hazelcast.core.Address`), new address if given address is known, otherwise returns null
        """
        if address is None:
            return None

        public_address = self._private_to_public.get(address)
        if public_address:
            return public_address

        self.refresh()

        return self._private_to_public.get(address)

    def refresh(self):
        """
        Refreshes the internal lookup table if necessary.
        """
        try:
            self._private_to_public = self.cloud_discovery.discover_nodes()
        except Exception as ex:
            self.logger.warning("Failed to load addresses from Hazelcast.cloud: {}".format(ex.args[0]))


class HazelcastCloudDiscovery(object):
    """
    Discovery service that discover nodes via Hazelcast.cloud
    https://coordinator.hazelcast.cloud/cluster/discovery?token=<TOKEN>
    """
    _CLOUD_URL_PATH = "/cluster/discovery?token="
    _PRIVATE_ADDRESS_PROPERTY = "private-address"
    _PUBLIC_ADDRESS_PROPERTY = "public-address"

    CLOUD_URL_BASE_PROPERTY = ClientProperty("hazelcast.client.cloud.url", "https://coordinator.hazelcast.cloud")
    """
    Internal client property to change base url of cloud discovery endpoint.
    Used for testing cloud discovery.
    """

    def __init__(self, host, url, connection_timeout):
        self._host = host
        self._url = url
        self._connection_timeout = connection_timeout
        # Default context operates only on TLSv1+, checks certificates,hostname and validity
        self._ctx = ssl.create_default_context()

    def discover_nodes(self):
        """
        Discovers nodes from Hazelcast.cloud.

        :return: (dict), Dictionary that maps private addresses to public addresses.
        """
        try:
            return self._call_service()
        except Exception as ex:
            raise ex

    def _call_service(self):
        try:
            https_connection = http_client.HTTPSConnection(host=self._host,
                                                           timeout=self._connection_timeout,
                                                           context=self._ctx)
            https_connection.request(method="GET", url=self._url, headers={"Accept-Charset": "UTF-8"})
            https_response = https_connection.getresponse()
        except ssl.SSLError as ex:
            raise HazelcastCertificationError(str(ex))
        self._check_error(https_response)
        return self._parse_response(https_response)

    def _check_error(self, https_response):
        response_code = https_response.status
        if response_code != 200:  # HTTP OK
            error_message = https_response.read().decode("utf-8")
            raise IOError(error_message)

    def _parse_response(self, https_response):
        json_value = json.loads(https_response.read().decode("utf-8"))
        private_to_public_addresses = dict()
        for value in json_value:
            private_address = value[self._PRIVATE_ADDRESS_PROPERTY]
            public_address = value[self._PUBLIC_ADDRESS_PROPERTY]

            public_addr = _parse_address(public_address)[0]
            private_to_public_addresses[Address(private_address, public_addr.port)] = public_addr

        return private_to_public_addresses

    @staticmethod
    def get_host_and_url(properties, cloud_token):
        """
        Helper method to get host and url that can be used in HTTPSConnection.

        :param properties: Client config properties.
        :param cloud_token: Cloud discovery token.
        :return: Host and URL pair
        """
        host = properties.get(HazelcastCloudDiscovery.CLOUD_URL_BASE_PROPERTY.name,
                              HazelcastCloudDiscovery.CLOUD_URL_BASE_PROPERTY.default_value)
        host = host.replace("https://", "")
        host = host.replace("http://", "")
        return host, HazelcastCloudDiscovery._CLOUD_URL_PATH + cloud_token
