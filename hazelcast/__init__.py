__version__ = "4.0"

# Set the default handler to "hazelcast" loggers
# to avoid "No handlers could be found" warnings.
import logging

try:
    from logging import NullHandler
except ImportError:

    class NullHandler(logging.Handler):
        def emit(self, record):
            pass


logging.getLogger(__name__).addHandler(NullHandler())

from hazelcast.client import HazelcastClient
