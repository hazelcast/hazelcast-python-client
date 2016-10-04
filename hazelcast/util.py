from __future__ import with_statement

import itertools
import threading
import time
from collections import Sequence, Iterable
from types import TypeType

from hazelcast.core import Address

DEFAULT_ADDRESS = "127.0.0.1"
DEFAULT_PORT = 5701


def check_not_none(val, message):
    """
    Tests if an argument is not ``None``.

    :param val: (object), the argument tested to see if it is not ``None``.
    :param message: (str), the error message.
    """
    if val is None:
        raise AssertionError(message)


def check_true(val, message):
    """
    Tests whether the provided expression is ``true``.

    :param val: (bool), the expression tested to see if it is ``true``.
    :param message: (str), the error message.
    """
    if not val:
        raise AssertionError(message)


def check_not_negative(val, message):
    """
    Tests if a value is not negative.

    :param val: (Number), the value tested to see if it is not negative.
    :param message: (str), the error message.
    """
    if val < 0:
        raise AssertionError(message)


def check_not_empty(collection, message):
    """
    Tests if a collection is not empty.

    :param collection: (Collection), the collection tested to see if it is not empty.
    :param message: (str), the error message.
    """
    if not collection:
        raise AssertionError(message)


def current_time():
    """
    Returns the current time of the system.

    :return: (float), current time of the system.
    """
    return time.time()


def thread_id():
    """
    Returns the current thread's id.

    :return: (int), current thread's id.
    """
    return threading.currentThread().ident


def to_millis(seconds):
    """
    Converts the time parameter in seconds to milliseconds. If the given time is negative, returns the original value.

    :param seconds: (Number), the given time in seconds.
    :return: (int), result of the conversation in milliseconds.
    """
    if seconds >= 0:
        return int(seconds * 1000)
    return seconds


def validate_type(_type):
    """
    Validates the type.

    :param _type: (Type), the type to be validated.
    """
    if not isinstance(_type, TypeType):
        raise ValueError("Serializer should be an instance of {}".format(_type.__name__))


def validate_serializer(serializer, _type):
    """
    Validates the serializer for given type.

    :param serializer: (Serializer), the serializer to be validated.
    :param _type: (Type), type to be used for serializer validation.
    """
    if not issubclass(serializer, _type):
        raise ValueError("Serializer should be an instance of {}".format(_type.__name__))


class AtomicInteger(object):
    """
    AtomicInteger is an Integer which can work atomically.
    """
    def __init__(self, initial=0):
        self.count = itertools.count(start=initial)

    def get_and_increment(self):
        """
        Returns the current value and increment it.

        :return: (int), current value of AtomicInteger.
        """
        return self.count.next()

    def set(self, value):
        """
        Sets the value of this AtomicInteger.
        :param value: (int), the new value of AtomicInteger.
        """
        self.count = itertools.count(start=value)


def enum(**enums):
    """
    Utility method for defining enums.
    :param enums: Parameters of enumeration.
    :return: (Enum), the created enumerations.
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


class ImmutableLazyDataList(Sequence):
    def __init__(self, list_data, to_object):
        super(ImmutableLazyDataList, self).__init__()
        self._list_data = list_data
        self._list_obj = [None] * len(self._list_data)
        self.to_object = to_object

    def __contains__(self, value):
        return super(ImmutableLazyDataList, self).__contains__(value)

    def __len__(self):
        return self._list_data.__len__()

    def __getitem__(self, index):
        val = self._list_obj[index]
        if not val:
            data = self._list_data[index]
            if isinstance(data, tuple):
                (key, value) = data
                self._list_obj[index] = (self.to_object(key), self.to_object(value))
            else:
                self._list_obj[index] = self.to_object(data)
            return self._list_obj[index]

    def __eq__(self, other):
        if not isinstance(other, Iterable):
            return False
        self._populate()
        return self._list_obj == other

    def _populate(self):
        for index, data in enumerate(self._list_data):
            if not self._list_obj[index]:
                self.__getitem__(index)

    def __repr__(self):
        self._populate()
        return str(self._list_obj)


# Serialization Utilities

def get_portable_version(portable, default_version):
    try:
        version = portable.get_class_version()
    except AttributeError:
        version = default_version
    return version
