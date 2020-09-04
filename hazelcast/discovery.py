import json
import logging

from hazelcast.errors import HazelcastCertificationError
from hazelcast.core import AddressHelper
from hazelcast.config import ClientProperty
from hazelcast.six.moves import http_client

try:
    import ssl
except ImportError:
    ssl = None


class HazelcastCloudAddressProvider(object):
    """
    Provides initial addresses for client to find and connect to a node
    and resolves private IP addresses of Hazelcast Cloud service.
    """
    logger = logging.getLogger("HazelcastClient.HazelcastCloudAddressProvider")

    def __init__(self, host, url, connection_timeout, logger_extras=None):
        self.cloud_discovery = HazelcastCloudDiscovery(host, url, connection_timeout)
        self._private_to_public = dict()
        self._logger_extras = logger_extras

    def load_addresses(self):
        """
        Loads member addresses from Hazelcast Cloud endpoint.

        :return: (Tuple), The possible member addresses as primary addresses to connect to.
        """
        try:
            nodes = self.cloud_discovery.discover_nodes()
            # Every private address is primary
            return list(nodes.keys()), []
        except Exception as ex:
            self.logger.warning("Failed to load addresses from Hazelcast Cloud: %s" % ex.args[0],
                                extra=self._logger_extras)
        return [], []

    def translate(self, address):
        """
        Translates the given address to another address specific to network or service.

        :param address: (:class:`~hazelcast.core.Address`), private address to be translated
        :return: (:class:`~hazelcast.core.Address`), new address if given address is known, otherwise returns None
        """
        if address is None:
            return None

        public_address = self._private_to_public.get(address, None)
        if public_address:
            return public_address

        self.refresh()

        return self._private_to_public.get(address, None)

    def refresh(self):
        """
        Refreshes the internal lookup table if necessary.
        """
        try:
            self._private_to_public = self.cloud_discovery.discover_nodes()
        except Exception as ex:
            self.logger.warning("Failed to load addresses from Hazelcast.cloud: {}".format(ex.args[0]),
                                extra=self._logger_extras)


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

            public_addr = AddressHelper.address_from_str(public_address)
            # If not explicitly given, create the private address with the public addresses port
            private_addr = AddressHelper.address_from_str(private_address, public_addr.port)
            private_to_public_addresses[private_addr] = public_addr

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
