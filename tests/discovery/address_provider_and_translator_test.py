from __future__ import with_statement

from unittest import TestCase
from hazelcast.address import AddressProvider, AddressTranslator


class AddressProviderAndTranslatorTest(TestCase):
    def test_load_addresses(self):
        provider = AddressProvider()
        with self.assertRaises(NotImplementedError):
            provider.load_addresses()

    def test_translate(self):
        translator = AddressTranslator()
        with self.assertRaises(NotImplementedError):
            translator.translate("")

    def test_refresh(self):
        translator = AddressTranslator()
        with self.assertRaises(NotImplementedError):
            translator.refresh()
