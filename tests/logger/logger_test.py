import datetime
import logging
import os

from inspect import currentframe, getframeinfo
from hazelcast import HazelcastClient
from hazelcast.config import LoggerConfig, ClientConfig
from hazelcast.six import StringIO
from tests.util import get_abs_path
from tests.base import HazelcastTestCase


class LoggerConfigTest(HazelcastTestCase):
    CUR_DIR = os.path.dirname(__file__)

    @classmethod
    def setUpClass(cls):
        cls.rc = cls.create_rc()
        cls.cluster = cls.create_cluster(cls.rc, None)
        cls.member = cls.cluster.start_member()

    @classmethod
    def tearDownClass(cls):
        cls.rc.exit()

    def test_default_config(self):
        logger_config = LoggerConfig()

        self.assertEqual(logging.INFO, logger_config.level)
        self.assertIsNone(logger_config.configuration_file)

        config = ClientConfig()
        config.logger_config = logger_config

        client = HazelcastClient(config)
        self.assertEqual(logging.INFO, client.logger.level)
        self.assertTrue(client.logger.isEnabledFor(logging.INFO))
        self.assertTrue(client.logger.isEnabledFor(logging.WARNING))
        self.assertTrue(client.logger.isEnabledFor(logging.ERROR))
        self.assertTrue(client.logger.isEnabledFor(logging.CRITICAL))

        out = StringIO()
        default_handler = client.logger.handlers[0]
        default_handler.stream = out

        client.logger.debug("DEBUG_TEST")
        client.logger.info("INFO_TEST")
        client.logger.error("ERROR_TEST")
        client.logger.critical("CRITICAL_TEST")

        out_str = out.getvalue()

        self.assertEqual(0, out_str.count("DEBUG_TEST"))
        self.assertEqual(1, out_str.count("INFO_TEST"))
        self.assertEqual(1, out_str.count("ERROR_TEST"))
        self.assertEqual(1, out_str.count("CRITICAL_TEST"))

        client.shutdown()

    def test_non_default_configuration_level(self):
        logger_config = LoggerConfig()

        logger_config.level = logging.CRITICAL

        self.assertEqual(logging.CRITICAL, logger_config.level)
        self.assertIsNone(logger_config.configuration_file)

        config = ClientConfig()
        config.logger_config = logger_config

        client = HazelcastClient(config)
        self.assertEqual(logging.CRITICAL, client.logger.level)
        self.assertFalse(client.logger.isEnabledFor(logging.INFO))
        self.assertFalse(client.logger.isEnabledFor(logging.WARNING))
        self.assertFalse(client.logger.isEnabledFor(logging.ERROR))
        self.assertTrue(client.logger.isEnabledFor(logging.CRITICAL))

        out = StringIO()
        default_handler = client.logger.handlers[0]
        default_handler.stream = out

        client.logger.debug("DEBUG_TEST")
        client.logger.info("INFO_TEST")
        client.logger.error("ERROR_TEST")
        client.logger.critical("CRITICAL_TEST")

        out_str = out.getvalue()

        self.assertEqual(0, out_str.count("DEBUG_TEST"))
        self.assertEqual(0, out_str.count("INFO_TEST"))
        self.assertEqual(0, out_str.count("ERROR_TEST"))
        self.assertEqual(1, out_str.count("CRITICAL_TEST"))

        client.shutdown()

    def test_simple_custom_logging_configuration(self):
        logger_config = LoggerConfig()

        # Outputs to stdout with the level of error
        config_path = get_abs_path(self.CUR_DIR, "simple_config.json")
        logger_config.configuration_file = config_path

        self.assertEqual(config_path, logger_config.configuration_file)

        config = ClientConfig()
        config.logger_config = logger_config

        client = HazelcastClient(config)
        self.assertEqual(logging.ERROR, client.logger.getEffectiveLevel())
        self.assertFalse(client.logger.isEnabledFor(logging.INFO))
        self.assertFalse(client.logger.isEnabledFor(logging.WARNING))
        self.assertTrue(client.logger.isEnabledFor(logging.ERROR))
        self.assertTrue(client.logger.isEnabledFor(logging.CRITICAL))

        out = StringIO()
        handler = logging.getLogger("HazelcastClient").handlers[0]
        handler.stream = out

        client.logger.debug("DEBUG_TEST")
        client.logger.info("INFO_TEST")
        client.logger.error("ERROR_TEST")
        client.logger.critical("CRITICAL_TEST")

        out_str = out.getvalue()

        self.assertEqual(0, out_str.count("DEBUG_TEST"))
        self.assertEqual(0, out_str.count("INFO_TEST"))
        self.assertEqual(1, out_str.count("ERROR_TEST"))
        self.assertEqual(1, out_str.count("CRITICAL_TEST"))

        client.shutdown()

    def test_default_configuration_multiple_clients(self):
        client1 = HazelcastClient()
        client2 = HazelcastClient()

        out1 = StringIO()
        out2 = StringIO()

        client1.logger.handlers[0].stream = out1
        client2.logger.handlers[0].stream = out2

        client1.logger.info("TEST_MSG")
        client2.logger.info("TEST_MSG")

        out1_str = out1.getvalue()
        out2_str = out2.getvalue()

        self.assertEqual(1, out1_str.count("TEST_MSG"))
        self.assertEqual(1, out2_str.count("TEST_MSG"))

    def test_same_custom_configuration_file_with_multiple_clients(self):
        config = ClientConfig()

        config_file = get_abs_path(self.CUR_DIR, "simple_config.json")
        config.logger_config.configuration_file = config_file

        client1 = HazelcastClient(config)
        client2 = HazelcastClient(config)

        out = StringIO()

        logging.getLogger("HazelcastClient").handlers[0].stream = out
        print(logging.getLogger("HazelcastClient").handlers)

        client1.logger.critical("TEST_MSG")
        client2.logger.critical("TEST_MSG")

        out_str = out.getvalue()

        self.assertEqual(2, out_str.count("TEST_MSG"))

    def test_multiple_clients_with_different_custom_configurations(self):
        config1 = ClientConfig()
        config2 = ClientConfig()

        config1.client_name = "MyClient1"
        config2.client_name = "MyClient2"

        clien1_config = get_abs_path(self.CUR_DIR, "client1_config.json")
        clien2_config = get_abs_path(self.CUR_DIR, "client2_config.json")

        config1.logger_config.configuration_file = clien1_config
        config2.logger_config.configuration_file = clien2_config

        client1 = HazelcastClient(config1)
        client2 = HazelcastClient(config2)

        out1 = StringIO()
        out2 = StringIO()

        client1.logger.handlers[0].stream = out1
        client2.logger.handlers[0].stream = out2

        client1.logger.info("CLIENT1_INFO_TEST")
        client1.logger.error("CLIENT1_ERROR_TEST")

        client2.logger.info("CLIENT2_INFO_TEST")
        client2.logger.error("CLIENT2_ERROR_TEST")

        out1_str = out1.getvalue()
        out2_str = out2.getvalue()

        self.assertEqual(1, out1_str.count("CLIENT1_INFO_TEST"))
        self.assertEqual(1, out1_str.count("CLIENT1_ERROR_TEST"))
        self.assertEqual(0, out2_str.count("CLIENT2_INFO_TEST"))
        self.assertEqual(1, out2_str.count("CLIENT2_ERROR_TEST"))

        client1.shutdown()
        client2.shutdown()

    def test_default_logger_output(self):
        client = HazelcastClient()

        out = StringIO()

        client.logger.handlers[0].stream = out
        version_message = client.logger.handlers[0].filters[0]._version_message.split(" ")

        client.logger.info("TEST_MSG")

        out_str = out.getvalue()

        print(out_str)

        self.assertTrue("TEST_MSG" in out_str)

        for line in out_str.split("\n"):
            if "TEST_MSG" in line:
                print(line)
                level_name, group_name, version, message = line.split(" ")
                self.assertEqual("INFO:", level_name)
                self.assertEqual(version_message[0], group_name)
                self.assertEqual(version_message[1], version)
                self.assertEqual("TEST_MSG", message)

    def test_custom_configuration_output(self):
        config = ClientConfig()
        config_file = get_abs_path(self.CUR_DIR, "detailed_config.json")

        config.logger_config.configuration_file = config_file
        config.client_name = "Detailed"

        client = HazelcastClient(config)

        std_out = StringIO()
        std_err = StringIO()

        for handler in client.logger.handlers:
            if handler.get_name() == "StdoutHandler":
                handler.stream = std_out
            else:
                handler.stream = std_err

        frame_info = getframeinfo(currentframe())
        client.logger.info("TEST_MSG")
        # These two statements above should
        # follow each other without a white space.
        # Otherwise, arrange the line number in
        # the assert statements accordingly.

        std_out_str = std_out.getvalue()
        std_err_str = std_err.getvalue()

        self.assertTrue("TEST_MSG" in std_out_str)
        self.assertTrue("TEST_MSG" in std_err_str)

        for line in std_out_str.split("\n"):
            if "TEST_MSG" in line:
                asc_time, name, level_name, message = line.split("*")
                self.assertTrue(self._is_valid_date_string(asc_time))
                self.assertEqual("HazelcastClient.Detailed", name)
                self.assertEqual("INFO", level_name)
                self.assertEqual("TEST_MSG", message)

        for line in std_err_str.split("\n"):
            if "TEST_MSG" in line:
                asc_time, name, func, line_no, level_name, message = line.split("*")
                self.assertTrue(self._is_valid_date_string(asc_time))
                self.assertEqual("HazelcastClient.Detailed", name)
                self.assertEqual(frame_info.function, func)
                self.assertEqual(str(frame_info.lineno + 1), line_no)
                self.assertEqual("INFO", level_name)
                self.assertEqual("TEST_MSG", message)

        client.shutdown()

    def _is_valid_date_string(self, time_str, fmt="%Y-%m-%d %H:%M:%S"):
        try:
            datetime.datetime.strptime(time_str, fmt)
        except ValueError:
            return False
        return True
