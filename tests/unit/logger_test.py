import logging
import unittest
from io import StringIO

from mock import MagicMock

from hazelcast.lifecycle import _InternalLifecycleService


class LoggerTest(unittest.TestCase):
    def test_null_handler_is_added_by_default(self):
        logger = logging.getLogger("hazelcast")
        self.assertGreater(len(logger.handlers), 0)

        found = False
        for handler in logger.handlers:
            if handler.__class__.__name__ == "NullHandler":
                # We check the class name instead of isinstance
                # check with the logging.NullHandler as the class,
                # since we might get an ImportError in __init__.py
                # and fallback to our class.
                found = True
                break

        self.assertTrue(found)

    def test_logging_when_handlers_are_added(self):
        out = StringIO()
        handler = logging.StreamHandler(out)
        logger = logging.getLogger("hazelcast")
        original_level = logger.level
        try:
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)

            self.assertEqual(0, len(out.getvalue()))

            # Start a service that should print a log
            service = _InternalLifecycleService(MagicMock())
            service.start()

            self.assertGreater(len(out.getvalue()), 0)  # Something is written to stream
        finally:
            logger.setLevel(original_level)
            logger.removeHandler(handler)

    def test_logging_when_handlers_are_added_to_root_logger(self):
        out = StringIO()
        handler = logging.StreamHandler(out)
        logger = logging.getLogger()
        original_level = logger.level
        try:
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)

            self.assertEqual(0, len(out.getvalue()))

            # Start a service that should print a log
            service = _InternalLifecycleService(MagicMock())
            service.start()

            self.assertGreater(len(out.getvalue()), 0)  # Something is written to stream
        finally:
            logger.setLevel(original_level)
            logger.removeHandler(handler)
