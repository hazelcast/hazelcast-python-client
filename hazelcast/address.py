from hazelcast.util import parse_addresses


class AddressProvider(object):
    """
    Provides initial addresses for client to find and connect to a node
    """
    def load_addresses(self):
        """
        :return: (Sequence), The possible member addresses to connect to.
        """
        raise NotImplementedError()


class AddressTranslator(object):
    """
    AddressTranslator is used to resolve private ip addresses of cloud services.
    """
    def translate(self, address):
        """
        Translates the given address to another address specific to network or service.
        :param address: (:class:`~hazelcast.core.Address`)
        :return: (:class:`~hazelcast.core.Address`), new address if given address is known, otherwise returns null
        """
        raise NotImplementedError()

    def refresh(self):
        """
        Refreshes the internal lookup table if necessary.
        """
        raise NotImplementedError()


class DefaultAddressProvider(AddressProvider):
    """
    Default address provider of Hazelcast.
    Loads addresses from the Hazelcast configuration.
    """
    def __init__(self, network_config, no_other_address_provider_exists):
        self._network_config = network_config
        self._no_other_address_provider_exists = no_other_address_provider_exists

    def load_addresses(self):
        return parse_addresses(self._network_config.addresses)


class DefaultAddressTranslator(AddressTranslator):
    """
    DefaultAddressTranslator is a no-op. It always returns the given address.
    """
    def translate(self, address):
        """
        :param address: (:class:`~hazelcast.core.Address`)
        :return: (class:`~hazelcast.core.Address`)
        """
        return address

    def refresh(self):
        return
