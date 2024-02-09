__version__ = "5.4.0"

# Set the default handler to "hazelcast" loggers
# to avoid "No handlers could be found" warnings.
import logging

logging.getLogger(__name__).addHandler(logging.NullHandler())

from hazelcast.client import HazelcastClient
