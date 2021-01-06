import json
import logging
import ssl

from hazelcast.errors import HazelcastCertificationError
from hazelcast.core import AddressHelper
from hazelcast.six.moves import http_client

_logger = logging.getLogger(__name__)


class HazelcastCloudAddressProvider(object):
    """Provides initial addresses for client to find and connect to a node
    and resolves private IP addresses of Hazelcast Cloud service.
    """

    def __init__(self, token, connection_timeout):
        self.cloud_discovery = HazelcastCloudDiscovery(token, connection_timeout)
        self._private_to_public = dict()

    def load_addresses(self):
        """Loads member addresses from Hazelcast Cloud endpoint.

        Returns:
            tuple[list[hazelcast.core.Address], list[hazelcast.core.Address]]: The possible member addresses
                as primary addresses to connect to.
        """
        try:
            nodes = self.cloud_discovery.discover_nodes()
            # Every private address is primary
            return list(nodes.keys()), []
        except Exception as e:
            _logger.warning("Failed to load addresses from Hazelcast Cloud: %s", e)
        return [], []

    def translate(self, address):
        """Translates the given address to another address specific to network or service.

        Args:
            address (hazelcast.core.Address): Private address to be translated

        Returns:
            hazelcast.core.Address: New address if given address is known, otherwise returns None
        """
        if address is None:
            return None

        public_address = self._private_to_public.get(address, None)
        if public_address:
            return public_address

        self.refresh()

        return self._private_to_public.get(address, None)

    def refresh(self):
        """Refreshes the internal lookup table if necessary."""
        try:
            self._private_to_public = self.cloud_discovery.discover_nodes()
        except Exception as e:
            _logger.warning("Failed to load addresses from Hazelcast.cloud: %s", e)


class HazelcastCloudDiscovery(object):
    """Discovery service that discover nodes via Hazelcast.cloud
    https://coordinator.hazelcast.cloud/cluster/discovery?token=<TOKEN>
    """

    _CLOUD_URL_BASE = "coordinator.hazelcast.cloud"
    _CLOUD_URL_PATH = "/cluster/discovery?token="
    _PRIVATE_ADDRESS_PROPERTY = "private-address"
    _PUBLIC_ADDRESS_PROPERTY = "public-address"

    def __init__(self, token, connection_timeout):
        self._url = self._CLOUD_URL_PATH + token
        self._connection_timeout = connection_timeout
        # Default context operates only on TLSv1+, checks certificates,hostname and validity
        self._ctx = ssl.create_default_context()

    def discover_nodes(self):
        """Discovers nodes from Hazelcast.cloud.

        Returns:
            dict[hazelcast.core.Address, hazelcast.core.Address]: Dictionary that maps private
                addresses to public addresses.
        """
        try:
            https_connection = http_client.HTTPSConnection(
                host=self._CLOUD_URL_BASE, timeout=self._connection_timeout, context=self._ctx
            )
            https_connection.request(
                method="GET", url=self._url, headers={"Accept-Charset": "UTF-8"}
            )
            https_response = https_connection.getresponse()
        except ssl.SSLError as err:
            raise HazelcastCertificationError(str(err))
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
