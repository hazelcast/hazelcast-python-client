from unittest import TestCase
from hazelcast.core import Address
from hazelcast.connection import DefaultAddressTranslator


class DefaultAddressTranslatorTest(TestCase):
    def setUp(self):
        self.translator = DefaultAddressTranslator()
        self.address = Address("192.168.0.1", 5701)

    def test_translate(self):
        actual = self.translator.translate(self.address)

        self.assertEqual(self.address, actual)

    def test_translate_none(self):
        actual = self.translator.translate(None)

        self.assertIsNone(actual)

    def test_refresh_and_translate(self):
        self.translator.refresh()
        actual = self.translator.translate(self.address)

        self.assertEqual(self.address, actual)
