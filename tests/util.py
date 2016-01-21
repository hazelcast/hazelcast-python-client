import logging
import time
from uuid import uuid4


def random_string():
    return str(uuid4())


def configure_logging(log_level=logging.INFO):
    logging.basicConfig(format='%(asctime)s%(msecs)03d [%(threadName)s][%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M:%S,")
    logging.getLogger().setLevel(log_level)


def event_collector():
    events = []

    def collector(e):
        events.append(e)

    collector.events = events
    return collector
