import random
import threading
import time
import logging
from collections import Sequence, Iterable

from hazelcast import six
from hazelcast.version import GIT_COMMIT_ID, GIT_COMMIT_DATE, CLIENT_VERSION

DEFAULT_ADDRESS = "127.0.0.1"
DEFAULT_PORT = 5701

MILLISECONDS_IN_SECONDS = 1000
NANOSECONDS_IN_SECONDS = 1e9


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


def current_time_in_millis():
    """
    Returns the current time of the system in millis.
    :return: (int), current time of the system in millis.
    """
    return to_millis(current_time())


def thread_id():
    """
    Returns the current thread's id.

    :return: (int), current thread's id.
    """
    return threading.currentThread().ident


def to_millis(seconds):
    """
    Converts the time parameter in seconds to milliseconds.

    :param seconds: (Number), the given time in seconds.
    :return: (int), result of the conversation in milliseconds.
    """
    return int(seconds * MILLISECONDS_IN_SECONDS)


def to_nanos(seconds):
    """
    Converts the time parameter in seconds to nanoseconds.

    :param seconds: (Number), the given time in seconds.
    :return: (int), result of the conversation in nanoseconds.
    """
    return int(seconds * NANOSECONDS_IN_SECONDS)


def validate_type(_type):
    """
    Validates the type.

    :param _type: (Type), the type to be validated.
    """
    if not isinstance(_type, type):
        raise ValueError("Serializer should be an instance of %s" % _type.__name__)


def validate_serializer(serializer, _type):
    """
    Validates the serializer for given type.

    :param serializer: (Serializer), the serializer to be validated.
    :param _type: (Type), type to be used for serializer validation.
    """
    if not issubclass(serializer, _type):
        raise ValueError("Serializer should be an instance of %s" % _type.__name__)


class AtomicInteger(object):
    """
    AtomicInteger is an Integer which can work atomically.
    """
    def __init__(self, initial=0):
        self._mux = threading.RLock()
        self._counter = initial

    def get_and_increment(self):
        """
        Returns the current value and increment it.

        :return: (int), current value of AtomicInteger.
        """
        with self._mux:
            res = self._counter
            self._counter += 1
            return res


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


class TimeUnit(object):
    """
    Represents the time durations at given units in seconds.
    """
    NANOSECOND = 1e-9
    MICROSECOND = 1e-6
    MILLISECOND = 1e-3
    SECOND = 1.0
    MINUTE = 60.0
    HOUR = 3600.0

    @staticmethod
    def to_seconds(value, time_unit):
        """
        :param value: (Number), value to be translated to seconds
        :param time_unit: Time duration in seconds
        :return: Value of the value in seconds
        """
        if isinstance(value, bool):
            # bool is a subclass of int. Don't let bool and float multiplication.
            raise TypeError
        return float(value) * time_unit


# Version utilities
UNKNOWN_VERSION = -1
MAJOR_VERSION_MULTIPLIER = 10000
MINOR_VERSION_MULTIPLIER = 100


def calculate_version(version_str):
    if not version_str:
        return UNKNOWN_VERSION

    main_parts = version_str.split("-")
    tokens = main_parts[0].split(".")

    if len(tokens) < 2:
        return UNKNOWN_VERSION

    try:
        major_coeff = int(tokens[0])
        minor_coeff = int(tokens[1])

        calculated_version = major_coeff * MAJOR_VERSION_MULTIPLIER + minor_coeff * MINOR_VERSION_MULTIPLIER

        if len(tokens) > 2:
            patch_coeff = int(tokens[2])
            calculated_version += patch_coeff

        return calculated_version
    except ValueError:
        return UNKNOWN_VERSION

# Logging utilities


DEFAULT_LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "filters": {
        "version_message_filter": {
            "()": "hazelcast.util.VersionMessageFilter"
        }
    },
    "formatters": {
        "hazelcast_formatter": {
            "()": "hazelcast.util.HazelcastFormatter",
            "format": "%(asctime)s %(name)s\n%(levelname)s: %(version_message)s %(message)s",
            "datefmt": "%b %d, %Y %I:%M:%S %p"
        }
    },
    "handlers": {
        "console_handler": {
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stderr",
            "filters": ["version_message_filter"],
            "formatter": "hazelcast_formatter"
        }
    },
    "loggers": {
        "HazelcastClient": {
            "handlers": ["console_handler"]
        }
    }
}


class VersionMessageFilter(logging.Filter):
    def filter(self, record):
        record.version_message = "[" + CLIENT_VERSION + "]"
        return True


class HazelcastFormatter(logging.Formatter):
    def format(self, record):
        client_name = getattr(record, "client_name", None)
        cluster_name = getattr(record, "cluster_name", None)
        if client_name and cluster_name:
            record.msg = "[" + cluster_name + "] [" + client_name + "] " + record.msg
        return super(HazelcastFormatter, self).format(record)


def create_git_info():
    if GIT_COMMIT_DATE:
        if GIT_COMMIT_ID:
            return "(" + GIT_COMMIT_DATE + " - " + GIT_COMMIT_ID + ") "
        return "(" + GIT_COMMIT_DATE + ") "

    if GIT_COMMIT_ID:
        return "(" + GIT_COMMIT_ID + ") "
    return ""


def to_list(*args, **kwargs):
    return list(*args, **kwargs)


def to_signed(unsigned, bit_len):
    mask = (1 << bit_len) - 1
    if unsigned & (1 << (bit_len - 1)):
        return unsigned | ~mask
    return unsigned & mask


def with_reserved_items(cls):
    reversed_mappings = {}
    for attr_name, attr_value in six.iteritems(vars(cls)):
        if not (attr_name.startswith("_") or callable(getattr(cls, attr_name))):
            reversed_mappings[attr_value] = attr_name

    class ClsWithReservedItems(cls):
        reverse = reversed_mappings

    return ClsWithReservedItems


number_types = (six.integer_types, float)
none_type = type(None)


class LoadBalancer(object):
    """Load balancer allows you to send operations to one of a number of endpoints (Members).
    It is up to the implementation to use different load balancing policies.

    If the client is configured with smart routing,
    only the operations that are not key based will be routed to the endpoint
    returned by the load balancer. If it is not, the load balancer will not be used.
    """
    def init(self, cluster_service):
        """
        Initializes the load balancer.

        :param cluster_service: (:class:`~hazelcast.cluster.ClusterService`), The cluster service to select members from
        """
        raise NotImplementedError("init")

    def next(self):
        """
        Returns the next member to route to.
        :return: (:class:`~hazelcast.core.Member`), Returns the next member or None if no member is available
        """
        raise NotImplementedError("next")


class _AbstractLoadBalancer(LoadBalancer):

    def __init__(self):
        self._cluster_service = None
        self._members = []

    def init(self, cluster_service):
        self._cluster_service = cluster_service
        cluster_service.add_listener(self._listener, self._listener, True)

    def _listener(self, _):
        self._members = self._cluster_service.get_members()


class RoundRobinLB(_AbstractLoadBalancer):
    """A load balancer implementation that relies on using round robin
    to a next member to send a request to.

    Round robin is done based on best effort basis, the order of members for concurrent calls to
    the next() is not guaranteed.
    """

    def __init__(self):
        super(RoundRobinLB, self).__init__()
        self._idx = 0

    def next(self):
        members = self._members
        if not members:
            return None

        n = len(members)
        idx = self._idx % n
        self._idx += 1
        return members[idx]


class RandomLB(_AbstractLoadBalancer):
    """A load balancer that selects a random member to route to.
    """

    def next(self):
        members = self._members
        if not members:
            return None
        idx = random.randrange(0, len(members))
        return members[idx]
