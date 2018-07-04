import json
import ssl
import logging

from hazelcast.address import AddressProvider, AddressTranslator
from hazelcast.exception import HazelcastCertificationError
from hazelcast.util import _parse_address
from hazelcast.core import Address
from hazelcast.six.moves import http_client


class HazelcastCloudAddressProvider(AddressProvider):
    logging.basicConfig()
    logger = logging.getLogger("HazelcastCloudAddressProvider")

    def __init__(self, host, url, connection_timeout):
        self._cloud_discovery = HazelcastCloudDiscovery(host, url, connection_timeout)

    def load_addresses(self):
        try:
            return list(self._cloud_discovery.discover_nodes().keys())
        except Exception as ex:
            self.logger.warning("Failed to load addresses from hazelcast.cloud : " + ex.args[0])
        return []


class HazelcastCloudAddressTranslator(AddressTranslator):
    logging.basicConfig()
    logger = logging.getLogger("HazelcastAddressTranslator")

    def __init__(self, host, url, connection_timeout):
        self._cloud_discovery = HazelcastCloudDiscovery(host, url, connection_timeout)
        self._private_to_public = dict()

    def translate(self, address):
        if address is None:
            return None
        public_address = self._private_to_public.get(address)
        if public_address is not None:
            return public_address
        self.refresh()
        public_address = self._private_to_public.get(address)
        if public_address is not None:
            return public_address
        return None

    def refresh(self):
        try:
            self._private_to_public = self._cloud_discovery.discover_nodes()
        except Exception as ex:
            self.logger.warning("Failed to load addresses from hazelcast.cloud : " + ex.args[0])


PROPERTY_CLOUD_URL_BASE = "hazelcast.client.cloud.url"
"""
Internal client property to change base url of cloud discovery endpoint.
Used for testing cloud discovery.
"""
DEFAULT_CLOUD_URL_BASE = "https://coordinator.hazelcast.cloud"


class HazelcastCloudDiscovery(object):
    """
    Discovery service that discover nodes via hazelcast.cloud
    https://coordinator.hazelcast.cloud/cluster/discovery?token=<TOKEN>
    """
    _CLOUD_URL_PATH = "/cluster/discovery?token="
    _PRIVATE_ADDRESS_PROPERTY = "private-address"
    _PUBLIC_ADDRESS_PROPERTY = "public-address"

    def __init__(self, host, url, connection_timeout):
        self._host = host
        self._url = url
        self._connection_timeout = connection_timeout

    def discover_nodes(self):
        try:
            return self._call_service()
        except Exception as ex:
            raise ex

    def _call_service(self):
        try:
            # Default context operates only on TLSv1+, checks certificates and hostname
            ssl_context = ssl.create_default_context()
            https_connection = http_client.HTTPSConnection(host=self._host, port=443, timeout=self._connection_timeout,
                                                           context=ssl_context)
            https_connection.request(method="GET", url=self._url, headers={"Accept-Charset": "UTF-8"})
            https_response = https_connection.getresponse()
        except Exception as ex:
            raise HazelcastCertificationError(ex.args[0])
        self._check_error(https_response)
        return self._parse_response(https_response)

    def _check_error(self, https_response):
        response_code = https_response.status
        if response_code != 200:  # HTTP OK
            error_message = self._extract_error_message(https_response)
            raise HazelcastCertificationError(error_message)

    @staticmethod
    def _extract_error_message(https_response):
        error_message = json.loads(https_response.read()).get("message")
        return "" if error_message is None else error_message

    def _parse_response(self, https_response):
        json_value = json.loads(https_response.read())
        private_to_public_addresses = dict()
        for value in json_value:
            private_address = value[self._PRIVATE_ADDRESS_PROPERTY]
            public_address = value[self._PUBLIC_ADDRESS_PROPERTY]
            public_addr = _parse_address(public_address)[0]
            private_to_public_addresses[Address(private_address, public_addr.port)] = public_addr
        return private_to_public_addresses

    @staticmethod
    def create_host_and_url(properties, cloud_token):
        host = properties.get(PROPERTY_CLOUD_URL_BASE, DEFAULT_CLOUD_URL_BASE)
        host = host.replace("https://", "")
        host = host.replace("http://", "")
        return host, HazelcastCloudDiscovery._CLOUD_URL_PATH + cloud_token
