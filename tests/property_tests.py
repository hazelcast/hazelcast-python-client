import os

from hazelcast.util import TimeUnit
from hazelcast.config import ClientProperty, ClientProperties, ClientConfig
from unittest import TestCase


class PropertyTest(TestCase):
    def test_client_property_defaults(self):
        prop = ClientProperty("name")
        self.assertEqual("name", prop.name)
        self.assertIsNone(prop.default_value)
        self.assertIsNone(prop.time_unit)

    def test_client_property(self):
        prop = ClientProperty("name", 0, TimeUnit.SECOND)
        self.assertEqual("name", prop.name)
        self.assertEqual(0, prop.default_value)
        self.assertEqual(TimeUnit.SECOND, prop.time_unit)

    def test_client_properties_with_config(self):
        config = ClientConfig()
        prop = ClientProperty("key")
        config.set_property(prop.name, "value")

        props = ClientProperties(config.get_properties())
        self.assertEqual("value", props.get(prop))

    def test_client_properties_with_default_value(self):
        config = ClientConfig()
        prop = ClientProperty("key", "def-value")

        props = ClientProperties(config.get_properties())
        self.assertEqual("def-value", props.get(prop))

    def test_client_properties_with_config_and_default_value(self):
        config = ClientConfig()
        prop = ClientProperty("key", "def-value")
        config.set_property(prop.name, "value")

        props = ClientProperties(config.get_properties())
        self.assertEqual("value", props.get(prop))

    def test_client_properties_with_environment_variable(self):
        environ = os.environ
        environ[ClientProperties.HEARTBEAT_INTERVAL.name] = "3000"

        props = ClientProperties(dict())
        self.assertEqual("3000", props.get(ClientProperties.HEARTBEAT_INTERVAL))
        os.unsetenv(ClientProperties.HEARTBEAT_INTERVAL.name)

    def test_client_properties_with_config_default_value_and_environment_variable(self):
        environ = os.environ
        prop = ClientProperties.HEARTBEAT_INTERVAL
        environ[prop.name] = "1000"

        config = ClientConfig()
        config.set_property(prop.name, 2000)

        props = ClientProperties(config.get_properties())
        self.assertEqual(2, props.get_seconds(prop))
        os.unsetenv(prop.name)

    def test_client_properties_get_second(self):
        config = ClientConfig()
        prop = ClientProperty("test", time_unit=TimeUnit.MILLISECOND)
        config.set_property(prop.name, 1000)

        props = ClientProperties(config.get_properties())
        self.assertEqual(1, props.get_seconds(prop))
    
    def test_client_properties_get_second_unsupported_type(self):
        config = ClientConfig()
        prop = ClientProperty("test", "value", TimeUnit.SECOND)
        config.set_property(prop.name, None)
        
        props = ClientProperties(config.get_properties())
        with self.assertRaises(ValueError):
            props.get_seconds(prop)

    def test_client_properties_get_second_positive(self):
        config = ClientConfig()
        prop = ClientProperty("test", 1000, TimeUnit.MILLISECOND)
        config.set_property(prop.name, -1000)

        props = ClientProperties(config.get_properties())
        self.assertEqual(1, props.get_seconds_positive_or_default(prop))

    def test_client_properties_get_second_positive_unsupported_type(self):
        config = ClientConfig()
        prop = ClientProperty("test", "value", TimeUnit.MILLISECOND)
        config.set_property(prop.name, None)

        props = ClientProperties(config.get_properties())
        with self.assertRaises(ValueError):
            props.get_seconds_positive_or_default(prop)
