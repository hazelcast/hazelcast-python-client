import datetime
import logging
import os

from inspect import currentframe, getframeinfo
from hazelcast import HazelcastClient
from hazelcast.config import LoggerConfig, ClientConfig
from hazelcast.six import StringIO
from hazelcast.version import CLIENT_VERSION
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
        self.assertIsNone(logger_config.config_file)

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

        out.flush()
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
        self.assertIsNone(logger_config.config_file)

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

        out.flush()
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
        logger_config.config_file = config_path

        self.assertEqual(config_path, logger_config.config_file)

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

        out.flush()
        out_str = out.getvalue()

        self.assertEqual(0, out_str.count("DEBUG_TEST"))
        self.assertEqual(0, out_str.count("INFO_TEST"))
        self.assertEqual(1, out_str.count("ERROR_TEST"))
        self.assertEqual(1, out_str.count("CRITICAL_TEST"))

        client.shutdown()

    def test_default_configuration_multiple_clients(self):
        client1 = HazelcastClient()
        client2 = HazelcastClient()

        out = StringIO()

        client1.logger.handlers[0].stream = out
        client2.logger.handlers[0].stream = out

        client1.logger.info("TEST_MSG")
        client2.logger.info("TEST_MSG")

        out.flush()
        out_str = out.getvalue()

        self.assertEqual(2, out_str.count("TEST_MSG"))

        client1.shutdown()
        client2.shutdown()

    def test_same_custom_configuration_file_with_multiple_clients(self):
        config = ClientConfig()

        config_file = get_abs_path(self.CUR_DIR, "simple_config.json")
        config.logger_config.configuration_file = config_file

        client1 = HazelcastClient(config)
        client2 = HazelcastClient(config)

        out = StringIO()

        logging.getLogger("HazelcastClient").handlers[0].stream = out

        client1.logger.critical("TEST_MSG")
        client2.logger.critical("TEST_MSG")

        out.flush()
        out_str = out.getvalue()

        self.assertEqual(2, out_str.count("TEST_MSG"))

        client1.shutdown()
        client2.shutdown()

    def test_default_logger_output(self):
        client = HazelcastClient()

        out = StringIO()

        client.logger.handlers[0].stream = out
        version_message = "[" + CLIENT_VERSION + "]"

        client.logger.info("TEST_MSG")

        out.flush()
        out_str = out.getvalue()

        self.assertTrue("TEST_MSG" in out_str)

        for line in out_str.split("\n"):
            if "TEST_MSG" in line:
                level_name, version, message = line.split(" ")
                self.assertEqual("INFO:", level_name)
                self.assertEqual(version_message, version)
                self.assertEqual("TEST_MSG", message)

        client.shutdown()

    def test_custom_configuration_output(self):
        config = ClientConfig()
        config_file = get_abs_path(self.CUR_DIR, "detailed_config.json")

        config.logger_config.config_file = config_file

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

        std_out.flush()
        std_err.flush()
        std_out_str = std_out.getvalue()
        std_err_str = std_err.getvalue()

        self.assertTrue("TEST_MSG" in std_out_str)
        self.assertTrue("TEST_MSG" in std_err_str)

        for line in std_out_str.split("\n"):
            if "TEST_MSG" in line:
                print(line)
                asc_time, name, level_name, message = line.split("*")
                self.assertTrue(self._is_valid_date_string(asc_time))
                self.assertEqual("HazelcastClient", name)
                self.assertEqual("INFO", level_name)
                self.assertEqual("TEST_MSG", message)

        for line in std_err_str.split("\n"):
            if "TEST_MSG" in line:
                asc_time, name, func, line_no, level_name, message = line.split("*")
                self.assertTrue(self._is_valid_date_string(asc_time))
                self.assertEqual("HazelcastClient", name)
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
