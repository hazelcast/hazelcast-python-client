from __future__ import with_statement
import threading

import itertools

from hazelcast.core import Address

DEFAULT_ADDRESS = "127.0.0.1"
DEFAULT_PORT = 5701


def check_not_none(val, message):
    if val is None:
        raise AssertionError(message)


class AtomicInteger(object):
    def __init__(self, initial=0):
        self.lock = threading.Lock()
        self.initial = initial

    def increment_and_get(self):
        with self.lock:
            self.initial += 1
            return self.initial


def enum(**enums):
    """
    Utility method for defining enums
    :param enums:
    :return:
    """
    enums['reverse'] = dict((value, key) for key, value in enums.iteritems())
    return type('Enum', (), enums)


def _parse_address(address):
    if ":" in address:
        host, port = address.split(":")
        return [Address(host, int(port))]
    return [Address(address, p) for p in xrange(DEFAULT_PORT, DEFAULT_PORT + 3)]


def get_possible_addresses(addresses=[], member_list=[]):
    address_lists = list(itertools.chain(*[_parse_address(a) for a in addresses]))
    return set((address_lists + [m.address for m in member_list])) or _parse_address(DEFAULT_ADDRESS)
