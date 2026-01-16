import asyncio
import logging

from hazelcast.discovery import HazelcastCloudDiscovery

_logger = logging.getLogger(__name__)


class HazelcastCloudAddressProvider:
    """Provides initial addresses for client to find and connect to a node
    and resolves private IP addresses of Hazelcast Cloud service.
    """

    def __init__(self, token, connection_timeout):
        self.cloud_discovery = HazelcastCloudDiscovery(token, connection_timeout)
        self._private_to_public = dict()

    async def load_addresses(self):
        """Loads member addresses from Hazelcast Cloud endpoint.

        Returns:
            tuple[list[hazelcast.core.Address], list[hazelcast.core.Address]]: The possible member addresses
                as primary addresses to connect to.
        """
        try:
            nodes = await asyncio.to_thread(self.cloud_discovery.discover_nodes)
            # Every private address is primary
            return list(nodes.keys()), []
        except Exception as e:
            _logger.warning("Failed to load addresses from Hazelcast Cloud: %s", e)
        return [], []

    async def translate(self, address):
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

        await self.refresh()

        return self._private_to_public.get(address, None)

    async def refresh(self):
        """Refreshes the internal lookup table if necessary."""
        try:
            self._private_to_public = self.cloud_discovery.discover_nodes()
        except Exception as e:
            _logger.warning("Failed to load addresses from Hazelcast Cloud: %s", e)
