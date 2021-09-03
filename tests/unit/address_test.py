import unittest

from hazelcast.core import AddressHelper


class AddressHelperTest(unittest.TestCase):
    v4_address = "127.0.0.1"
    v6_address = "2001:0db8:85a3:0000:0000:8a2e:0370:7334"
    localhost = "localhost"
    port = 8080
    default_port = 5701
    default_port_count = 3

    def test_v4_address_with_port(self):
        self._validate_with_port(self.v4_address + ":P" + str(self.port), self.v4_address, self.port)

    def test_v4_address_without_port(self):
        self._validate_without_port(self.v4_address, self.v4_address)

    def test_v6_address_with_port(self):
        self._validate_with_port(
            "[" + self.v6_address + "]:" + str(self.port), self.v6_address, self.port
        )

    def test_v6_address_without_port(self):
        self._validate_without_port(self.v6_address, self.v6_address)

    def test_v6_address_without_port_with_brackets(self):
        self._validate_without_port("[" + self.v6_address + "]", self.v6_address)

    def test_localhost_with_port(self):
        self._validate_with_port(self.localhost + ":" + str(self.port), self.localhost, self.port)

    def test_localhost_without_port(self):
        self._validate_without_port(self.localhost, self.localhost)

    def _validate_with_port(self, address, host, port):
        primaries, secondaries = AddressHelper.get_possible_addresses(address)
        self.assertEqual(1, len(primaries))
        self.assertEqual(0, len(secondaries))

        address = primaries[0]
        self.assertEqual(host, address.host)
        self.assertEqual(port, address.port)

    def _validate_without_port(self, address, host):
        primaries, secondaries = AddressHelper.get_possible_addresses(address)
        self.assertEqual(1, len(primaries))
        self.assertEqual(self.default_port_count - 1, len(secondaries))

        for i in range(self.default_port_count):
            if i == 0:
                address = primaries[i]
            else:
                address = secondaries[i - 1]
            self.assertEqual(host, address.host)
            self.assertEqual(self.default_port + i, address.port)
