import random
import threading
import time
from collections import Sequence, Iterable

from hazelcast import six

DEFAULT_ADDRESS = "127.0.0.1"
DEFAULT_PORT = 5701

MILLISECONDS_IN_SECONDS = 1000
NANOSECONDS_IN_SECONDS = 1e9


def check_not_none(val, message):
    if val is None:
        raise AssertionError(message)


def check_true(val, message):
    if not val:
        raise AssertionError(message)


def check_not_negative(val, message):
    if val < 0:
        raise AssertionError(message)


def check_not_empty(collection, message):
    if not collection:
        raise AssertionError(message)


def check_is_number(val):
    if not isinstance(val, number_types):
        raise AssertionError("Number value expected")


def check_is_int(val):
    if not isinstance(val, six.integer_types):
        raise AssertionError("Int value expected")


def current_time():
    return time.time()


def current_time_in_millis():
    return to_millis(current_time())


def thread_id():
    return threading.currentThread().ident


def to_millis(seconds):
    if seconds is None:
        return -1
    return int(seconds * MILLISECONDS_IN_SECONDS)


def to_nanos(seconds):
    return int(seconds * NANOSECONDS_IN_SECONDS)


def validate_type(_type):
    if not isinstance(_type, type):
        raise ValueError("Serializer should be an instance of %s" % _type.__name__)


def validate_serializer(serializer, _type):
    if not issubclass(serializer, _type):
        raise ValueError("Serializer should be an instance of %s" % _type.__name__)


class AtomicInteger(object):
    """An Integer which can work atomically."""

    def __init__(self, initial=0):
        self._mux = threading.RLock()
        self._counter = initial

    def get_and_increment(self):
        """Returns the current value and increment it.

        Returns:
            int: Current value of AtomicInteger.
        """
        with self._mux:
            res = self._counter
            self._counter += 1
            return res

    def get(self):
        """Returns the current value.

        Returns:
            int: The current value.
        """
        with self._mux:
            return self._counter

    def add(self, count):
        """Adds the given value to the current value.
        Args:
            count (int): The value to add.
        """
        with self._mux:
            self._counter += count


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

        calculated_version = (
            major_coeff * MAJOR_VERSION_MULTIPLIER + minor_coeff * MINOR_VERSION_MULTIPLIER
        )

        if len(tokens) > 2:
            patch_coeff = int(tokens[2])
            calculated_version += patch_coeff

        return calculated_version
    except ValueError:
        return UNKNOWN_VERSION


def to_list(*args, **kwargs):
    return list(*args, **kwargs)


def to_signed(unsigned, bit_len):
    mask = (1 << bit_len) - 1
    if unsigned & (1 << (bit_len - 1)):
        return unsigned | ~mask
    return unsigned & mask


def get_attr_name(cls, value):
    for attr_name, attr_value in six.iteritems(vars(cls)):
        if attr_value == value:
            return attr_name
    return None


def _get_enum_value(cls, key):
    if isinstance(key, six.string_types):
        return getattr(cls, key, None)
    return None


def try_to_get_enum_value(value, enum_class):
    if get_attr_name(enum_class, value):
        # If the value given by the user corresponds
        # to value of the one of the enum members,
        # it is okay to set it directly
        return value
    else:
        # We couldn't find a enum member name for the
        # given value. Try to match the given config
        # option with enum member names and get value
        # associated with it.
        enum_value = _get_enum_value(enum_class, value)
        if enum_value is not None:
            return enum_value
        else:
            raise TypeError(
                "%s must be equal to one of the values or "
                "names of the members of the %s" % (value, enum_class.__name__)
            )


number_types = (six.integer_types, float)
none_type = type(None)


class LoadBalancer(object):
    """Load balancer allows you to send operations to one of a number of endpoints (Members).
    It is up to the implementation to use different load balancing policies.

    If the client is configured with smart routing,
    only the operations that are not key based will be routed to the endpoint
    """

    def init(self, cluster_service):
        """Initializes the load balancer.

        Args:
            cluster_service (hazelcast.cluster.ClusterService): The cluster service to select members from
        """
        raise NotImplementedError("init")

    def next(self):
        """Returns the next member to route to.

        Returns:
            hazelcast.core.MemberInfo: the next member or ``None`` if no member is available.
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
    """A load balancer that selects a random member to route to."""

    def next(self):
        members = self._members
        if not members:
            return None
        idx = random.randrange(0, len(members))
        return members[idx]


class IterationType:
    """To differentiate users selection on result collection on map-wide
    operations like ``entry_set``, ``key_set``, ``values`` etc.
    """

    KEY = 0
    """Iterate over keys"""

    VALUE = 1
    """Iterate over values"""

    ENTRY = 2
    """Iterate over entries"""
