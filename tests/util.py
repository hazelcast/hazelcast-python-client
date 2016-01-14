import logging
import time
from uuid import uuid4


def random_string():
    return str(uuid4())


def configure_logging(log_level=logging.INFO):
    logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
    logging.getLogger().setLevel(log_level)


def assert_true_eventually(assertion, timeout=30):
    timeout_time = time.time() + timeout
    while time.time() < timeout_time:
        try:
            assertion()
            return
        except AssertionError:
            time.sleep(0.1)
    raise
